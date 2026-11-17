import copy
from fastapi import HTTPException
from src.config.database import Session
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.models.revocation import Revocation
from src.utils.serialization import record
from src.services.migration import (digest,key_digest,make_backup,migrate,
 normalize_code,code_fields,source_code,filter_rows_by_codes)

def active_codes(s):
 """仅已落库完成的撤销单才参与过滤；processing 尚未提交，不影响其他事务。"""
 return {r.code for r in s.query(Revocation).filter_by(status='completed').all()}

def list_items():
 with Session() as s:return [record(r) for r in s.query(Revocation).order_by(Revocation.id.desc()).limit(100).all()]

def request_purge(data,user):
 """按规范化委托号发起撤销清除；同号重试幂等：已完成直接返回，处理中原样返回，失败可重试且不重复记完成事件。
 返回 (撤销单记录, 元信息)；created 表示首次建单，completed_now 表示本次请求真正完成了清除。"""
 code=normalize_code(data.code)
 if not code:raise HTTPException(422,'委托号规范化后为空，无法定位委托')
 with Session() as s:
  rev=s.query(Revocation).filter_by(code=code).with_for_update().first()
  if rev:
   if rev.status in ('processing','completed'):return record(rev),{'created':False,'completed_now':False}
   rev.status='processing';rev.error=None;rev.summary=None;created=False;s.commit()
  else:
   rev=Revocation(code=code,requested_code=str(data.code).strip()[:160],status='processing',actor=user.email)
   s.add(rev);s.commit();s.refresh(rev);created=True
  rev_id=rev.id
 try:
  summary=purge(rev_id,code)
 except Exception as exc:
  with Session() as s:
   r=s.get(Revocation,rev_id);r.status='failed';r.error=str(exc)[:1000];s.commit()
  raise HTTPException(500,'撤销清除失败，整次已回滚：'+str(exc)[:300])
 with Session() as s:
  r=s.get(Revocation,rev_id);r.status='completed';r.summary=summary;r.error=None;s.commit();s.refresh(r)
  return record(r),{'created':created,'completed_now':True}

def purge(rev_id,code):
 """单事务清除该规范化委托号的全部痕迹：批次原行、各运行快照、拒绝区与目标行，并重建对账与备份。"""
 with Session() as s:
  rev=s.query(Revocation).filter_by(id=rev_id).with_for_update().first()
  if not rev:raise RuntimeError('撤销单不存在')
  codes=active_codes(s)|{code}
  batches=s.query(Batch).order_by(Batch.id).with_for_update().all()
  mappings=s.query(Mapping).order_by(Mapping.id).with_for_update().all()
  runs=s.query(Run).order_by(Run.id).with_for_update().all()
  fields_by_batch={}
  for m in mappings:
   fields_by_batch.setdefault(m.batch_id,set()).update(code_fields(m.rules))
  batch_hits=[];total_batch_rows=0
  for b in batches:
   fields=fields_by_batch.get(b.id,set())
   kept,dropped=filter_rows_by_codes(b.rows,fields,codes)
   if dropped:
    b.rows=kept
    batch_hits.append({'batch_id':b.id,'batch_name':b.name,'dropped_rows':dropped})
    total_batch_rows+=dropped
  run_hits=[];target_deleted=0;snapshot_dropped=0
  for r in runs:
   snap=copy.deepcopy(r.snapshot or {})
   batch_snap=snap.get('batch') or {}
   mapping_snap=snap.get('mapping') or {}
   fields=code_fields(mapping_snap.get('rules') or [])
   original=batch_snap.get('rows') or []
   kept,dropped=filter_rows_by_codes(original,fields,codes)
   if not dropped:continue
   # 深拷贝后整体回赋，保证 SQLAlchemy JSON 变异追踪在 SQLite/PostgreSQL 上都能识别变化
   batch_snap['rows']=kept;snap['batch']=batch_snap;r.snapshot=snap;snapshot_dropped+=dropped
   hit={'run_id':r.id,'run_name':r.name,'status':r.status,'dropped_snapshot_rows':dropped}
   if r.status=='completed' and isinstance(r.result,dict):
    # 用过滤后的冻结输入重跑同一套映射，重建接受/拒绝/分块/摘要，生成与剩余数据匹配的新对账和备份
    new_result=migrate(kept,mapping_snap['rules'],mapping_snap['chunk_size'])
    old_checksum=r.result.get('checksum')
    ancestors=list(dict.fromkeys((r.result.get('ancestor_checksums') or [])+([old_checksum] if old_checksum else [])))
    new_result['ancestor_checksums']=ancestors
    new_result['backup']=make_backup(new_result,r.id,batch_snap.get('name',''))
    deleted=s.query(TargetRow).filter_by(run_id=r.id).delete()
    for i,row in enumerate(new_result['accepted_rows']):
     s.add(TargetRow(run_id=r.id,ordinal=i,key_digest=key_digest(row),payload=row))
    s.flush()
    actual=[x.payload for x in s.query(TargetRow).filter_by(run_id=r.id).order_by(TargetRow.ordinal)]
    if digest(actual)!=new_result['checksum']:raise RuntimeError('运行%s清除后回读校验失败'%r.id)
    r.result=new_result  # 新对象整体回赋，完成标记与清除数据同一事务落库
    target_deleted+=deleted
    hit.update({'target_rows_replaced':deleted,'new_checksum':new_result['checksum'],'old_checksum':old_checksum,
                'accepted_count':new_result['accepted_count'],'rejected_count':new_result['rejected_count']})
   run_hits.append(hit)
  s.commit()
 return {'code':code,'batch_rows_cleared':total_batch_rows,'snapshot_rows_cleared':snapshot_dropped,
         'target_rows_replaced':target_deleted,'batches':batch_hits,'runs':run_hits,
         'batches_scanned':len(batches),'runs_scanned':len(runs)}
