# Schema v2 · 误收委托撤销清除

新增 `revocations` 表：`id / code(unique) / requested_code / status(processing|completed|failed) / actor / summary(JSON) / error / created_at / updated_at`。`code` 为规范化委托号（去全部空白并大写），唯一约束把同号重复请求收敛为同一张撤销单，支撑幂等重试；完成标记与清除数据在同一事务提交，失败整次回滚。

清除范围（单事务、行锁串行化）：
- `batches.rows`：剔除该规范化委托号的全部原行（同号多行一并清除），其他委托原样保留。
- `runs.snapshot.batch.rows`：冻结快照同步剔除，保证旧运行再次排队也带不回该委托。
- `runs.result`（completed 运行）：用剔除后的冻结输入按同一映射重算，重建接受行、拒绝区（该委托拒绝行消失，其他拒绝行保留）、分块证据、逐行哈希与 checksum，并生成匹配的新逻辑备份；`ancestor_checksums` 追加清除前 checksum，使清除前下载的旧备份仍可通过摘要核验（逐行哈希与篡改校验不变，伪造自洽备份因不在摘要链上被拒绝）。
- `target_rows`：删除后按新结果整体重写，ordinal 重排，回读校验新 checksum，不一致即抛错回滚。

并发控制：撤销清除按 batches → mappings → runs 顺序加 `FOR UPDATE` 行锁；批次编辑、运行创建遵循同一加锁顺序；worker 在最终提交的 run 行锁内重读已生效撤销单并重算过滤，清除与迁移落库竞争时已撤销委托绝不落目标表。`revocations.status='completed'` 才参与过滤，`processing` 未提交不影响其他事务。

无独立 DDL 脚本，沿用 `Base.metadata.create_all` 建表；已存在的库需手动创建 `revocations` 表（定义见 `src/models/revocation.py`）。
