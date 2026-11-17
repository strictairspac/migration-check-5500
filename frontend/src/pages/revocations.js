import {store} from '../stores/business.js';
import {esc,json} from '../utils/html.js';
import {table} from '../components/common/table.js';
import {status} from '../components/common/status.js';

export function revocationsPage(){
 return `<div class="split"><section><h2>误收委托 · 撤销清除</h2><p>按规范化委托号（去除空白并统一大写）清除该委托的全部痕迹：各批次原行、运行快照、拒绝区与目标行均不再保留，其他委托保持原样。清除与批次编辑、迁移落库、备份恢复可并发；失败整次回滚。</p>
 <form id="revocation-form"><label>委托号（支持带空格/小写的同号输入）<input name="code" required placeholder="JS-20261117-001" maxlength="160"></label>
 <small>同号重复提交是幂等重试：已完成不重复清除、不重复记事件；失败可重试。空结果同样有效。</small>
 <button type="submit">发起撤销清除 →</button></form>
 <ol class="steps"><li>按规范化委托号定位全部批次与运行</li><li>单事务清除原行、快照、拒绝区与目标行</li><li>重建对账摘要与匹配的新逻辑备份</li><li>完成标记与清除数据同步落库，可查状态</li></ol></section>
 <section><h2>撤销清除单 <em>${store.revocations.length}</em></h2>
 ${table(['撤销单 / 委托号','状态','清除证据 / 错误'],store.revocations.map(rv=>[
  `<b>${esc(rv.code)}</b><small>${esc(rv.actor)}<br>${esc(rv.created_at)}</small>`,
  status(rv.status),
  rv.summary
   ? `<small>批次原行 ${rv.summary.batch_rows_cleared} · 快照行 ${rv.summary.snapshot_rows_cleared} · 目标替换 ${rv.summary.target_rows_replaced}</small><details><summary>展开清除证据</summary><pre>${json(rv.summary)}</pre></details>`
   : (rv.error?`<div class="feedback error">${esc(rv.error)}</div>`:'<small>处理中，页面自动刷新…</small>')
 ]))}
 <aside class="note">清除前下载的备份仍可回传：系统先核验原摘要与逐行哈希（篡改依旧拒绝），再恢复其中未撤销部分，并生成与当前数据匹配的新对账与备份。</aside></section></div>`;
}
