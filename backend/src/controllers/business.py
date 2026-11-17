from src.services import batches,mappings,runs,revocations
from src.services.audit import log
def save_batch(data,user,item_id=None):
 row=batches.save(data,item_id)
 dropped=row.pop('revoked_rows_dropped',0)
 log(user.email,'batch.save',row['name']+('；撤销委托拦截'+str(dropped)+'行' if dropped else ''))
 return row
def save_mapping(data,user,item_id=None):
 row=mappings.save(data,item_id);log(user.email,'mapping.save',row['name']);return row
def start_run(data,user):
 row=runs.create(data)
 dropped=row.pop('revoked_rows_dropped',0)
 log(user.email,'run.queue',row['name']+('；撤销委托拦截'+str(dropped)+'行' if dropped else ''))
 return row
def purge_revocation(data,user):
 row,meta=revocations.request_purge(data,user)
 if meta['created']:log(user.email,'revocation.request',row['code'])
 if meta['completed_now']:log(user.email,'revocation.completed',str(row['id'])+' '+str(row.get('summary')))
 return row
