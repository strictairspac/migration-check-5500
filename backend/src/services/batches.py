from fastapi import HTTPException
from src.config.database import Session
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.utils.serialization import record
from src.services.migration import code_fields,filter_rows_by_codes
from src.services.revocations import active_codes

def list_items():
 with Session() as s:return [record(r) for r in s.query(Batch).order_by(Batch.id.desc()).all()]
def save(data,item_id=None):
 with Session() as s:
  row=s.get(Batch,item_id) if item_id else Batch()
  if not row:raise HTTPException(404,'未找到记录')
  payload=data.model_dump()
  # 与撤销清除并发：锁该批次及其映射，按已生效撤销单隔离原行，已撤销委托无法经再次导入/编辑带回
  s.query(Batch).filter_by(id=row.id).with_for_update().first() if item_id else None
  mappings=s.query(Mapping).filter_by(batch_id=item_id).order_by(Mapping.id).with_for_update().all() if item_id else []
  fields=set()
  for m in mappings:fields.update(code_fields(m.rules))
  kept,dropped=filter_rows_by_codes(payload['rows'],fields,active_codes(s))
  payload['rows']=kept
  for k,v in payload.items():setattr(row,k,v)
  s.add(row);s.commit();s.refresh(row)
  out=record(row);out['revoked_rows_dropped']=dropped;return out
