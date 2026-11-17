import redis
from datetime import datetime,timezone
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.utils.serialization import record
from src.services.migration import (digest,key_digest,make_backup,migrate,validate_backup,
 code_fields,filter_rows_by_codes)
from src.services.revocations import active_codes
queue=redis.Redis.from_url(REDIS_URL)
def list_items():
 with Session() as s:return [record(r) for r in s.query(Run).order_by(Run.id.desc()).limit(100).all()]
def detail(item_id):
 with Session() as s:
  r=s.get(Run,item_id)
  if not r:raise HTTPException(404,'未找到对账运行')
  return record(r)
def create(data):
 with Session() as s:
  m=s.get(Mapping,data.mapping_id)
  if not m:raise HTTPException(404,'未找到字段映射')
  # 与撤销清除并发：按全局顺序先锁批次再锁映射（与purge一致，避免死锁），已撤销委托不进入任何新排队运行
  b=s.query(Batch).filter_by(id=m.batch_id).with_for_update().first()
  s.query(Mapping).filter_by(id=m.id).with_for_update().first()
  codes=active_codes(s)
  kept,dropped=filter_rows_by_codes(b.rows,code_fields(m.rules),codes)
  frozen_batch=record(b);frozen_batch['rows']=kept
  r=Run(name=data.name,batch_id=b.id,mapping_id=m.id,status='queued',snapshot={'batch':frozen_batch,'mapping':record(m)})
  s.add(r);s.commit();s.refresh(r);result=record(r);result['revoked_rows_dropped']=dropped
 try:queue.rpush('migration-runs',result['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(Run,result['id']);r.status='failed';r.error='队列不可用，请创建新任务';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 return result
def cancel(item_id):
 with Session() as s:
  r=s.query(Run).filter_by(id=item_id).with_for_update().first()
  if not r:raise HTTPException(404,'未找到对账运行')
  if r.status not in ('queued','running'):raise HTTPException(409,'运行已终止，无法取消')
  r.cancel_requested=True
  if r.status=='queued':r.status='cancelled'
  s.commit();return record(r)
def targets(item_id):
 detail(item_id)
 with Session() as s:rows=[r.payload for r in s.query(TargetRow).filter_by(run_id=item_id).order_by(TargetRow.ordinal)]
 return {'rows':rows,'count':len(rows),'checksum':digest(rows)}
def backup(item_id):
 r=detail(item_id)
 if r['status']!='completed':raise HTTPException(409,'仅完成运行可以备份')
 return r['result']['backup']
def restore(item_id,data):
 with Session() as s:
  r=s.query(Run).filter_by(id=item_id).with_for_update().first()
  if not r:raise HTTPException(404,'未找到对账运行')
  if r.status!='completed':raise HTTPException(409,'仅完成运行可以恢复')
  # 清除前下载的旧备份：当前摘要或撤销前祖先摘要任一匹配即通过；逐行/篡改校验仍然完整
  expected=[r.result['checksum']]+list(r.result.get('ancestor_checksums') or [])
  try:rows=validate_backup(data.backup,expected,item_id)
  except ValueError as e:raise HTTPException(422,str(e))
  before=s.query(TargetRow).filter_by(run_id=item_id).count()
  mapping_snap=r.snapshot['mapping'];batch_snap=r.snapshot['batch']
  fields=code_fields(mapping_snap['rules'])
  codes=active_codes(s)
  kept_backup,dropped=filter_rows_by_codes(rows,fields,codes)
  # 用冻结输入按同一套映射独立重算未撤销部分，作为回传备份可信性的独立证据
  rebuilt=migrate(batch_snap['rows'],mapping_snap['rules'],mapping_snap['chunk_size'])
  rebuilt_keys=[key_digest(x) for x in rebuilt['accepted_rows']]
  backup_keys=[key_digest(x) for x in kept_backup]
  if rebuilt_keys!=backup_keys or digest(kept_backup)!=digest(rebuilt['accepted_rows']):
   raise HTTPException(422,'备份未撤销部分与冻结输入重算结果不一致，拒绝恢复')
  s.query(TargetRow).filter_by(run_id=item_id).delete()
  for i,row in enumerate(kept_backup):s.add(TargetRow(run_id=item_id,ordinal=i,key_digest=key_digest(row),payload=row))
  s.flush()
  actual=[x.payload for x in s.query(TargetRow).filter_by(run_id=item_id).order_by(TargetRow.ordinal)]
  new_checksum=digest(actual)
  if new_checksum!=rebuilt['checksum'] or new_checksum!=digest(kept_backup):raise HTTPException(409,'恢复后校验失败，已回滚')
  old_checksum=r.result.get('checksum')
  ancestors=list(dict.fromkeys((r.result.get('ancestor_checksums') or [])+([old_checksum] if old_checksum and old_checksum!=new_checksum else [])))
  rebuilt['ancestor_checksums']=ancestors
  rebuilt['backup']=make_backup(rebuilt,item_id,batch_snap.get('name',''))
  r.result=rebuilt
  proof={'restored_at':datetime.now(timezone.utc).isoformat(),'previous_count':before,'restored_count':len(actual),
         'revoked_rows_excluded':dropped,'checksum':new_checksum,'verified':new_checksum==rebuilt['checksum'],
         'source_backup_checksum':data.backup.get('checksum')}
  r.recovery=(r.recovery or [])+[proof];s.commit();return proof
