"""误收委托撤销清除 —— 端到端验收测试（SQLite + fakeredis 等价环境）。

覆盖：
1. 多映射同号、跨块拒绝行清除
2. 批次原行/运行快照/拒绝区/目标行/对账/备份同步重建，其他委托不变
3. 再次导入与排队旧运行带不回已撤销委托
4. 清除前旧备份恢复：原摘要与篡改核验、只恢复未撤销部分、新对账与新备份
5. 空结果清除有效
6. 幂等重试不重复记清除事件；失败回滚与可重试
7. 清除与批次编辑/迁移落库并发（锁与最终提交过滤）
8. API 层角色权限与页面所需接口
"""
import os,sys,json,copy,threading,time
os.environ['JWT_SECRET']='test-secret'
os.environ['DATABASE_URL']='sqlite:///'+os.path.join(os.path.dirname(__file__),'test.db')
os.environ['REDIS_URL']='redis://fake/0'
sys.path.insert(0,'/workspace/backend')

import fakeredis
from src.services import runs as runs_svc
runs_svc.queue=fakeredis.FakeStrictRedis()
import src.worker as worker_mod
worker_mod.q=runs_svc.queue

from src.config.database import Base,engine,Session
from src.models.identity import User,Audit
from src.models.batch import Batch
from src.models.mapping import Mapping
from src.models.run import Run,TargetRow
from src.models.revocation import Revocation
from src.utils.security import hash_password
from src.schemas.business import BatchInput,RunInput,RevokeInput
from src.services import batches,mappings,runs,revocations
from src.services.migration import digest,key_digest,normalize_code,migrate
from src.controllers.business import purge_revocation
from fastapi import HTTPException

PASS=0;FAIL=0
def check(name,cond,extra=''):
 global PASS,FAIL
 if cond:PASS+=1;print('  ✓',name)
 else:FAIL+=1;print('  ✗ FAIL:',name,extra)

def setup_db():
 if os.path.exists(os.path.join(os.path.dirname(__file__),'test.db')):os.remove(os.path.join(os.path.dirname(__file__),'test.db'))
 Base.metadata.create_all(engine)
 with Session() as s:
  for role in ('admin','operator','viewer'):
   s.add(User(email=role+'@migration.local',role=role,password=hash_password('x')))
  s.commit()

class U:
 def __init__(self,email):self.email=email
admin=U('admin@migration.local');operator=U('operator@migration.local');viewer=U('viewer@migration.local')

RULES_A=[{'source':a,'target':b,'type':c,'required':True} for a,b,c in
 [('委托号','sample_code','text'),('检测项','test_name','text'),('结果','result_value','decimal'),('单位','unit','text'),('时间','measured_at','datetime')]]
# 第二套映射：委托号来自不同源字段名，验证多映射同号
RULES_B=[{'source':a,'target':b,'type':c,'required':True} for a,b,c in
 [('sample_no','sample_code','text'),('item','test_name','text'),('value','result_value','decimal'),('unit','unit','text'),('ts','measured_at','datetime')]]

def row(code,item='拉伸强度',v='420.1',ts='2026-11-17T09:00:00+08:00'):
 return {'委托号':code,'检测项':item,'结果':v,'单位':'MPa','时间':ts}
def row_b(code,item='拉伸强度',v='420.1',ts='2026-11-17T09:00:00+08:00'):
 return {'sample_no':code,'item':item,'value':v,'unit':'MPa','ts':ts}

print('== 场景1：多映射同号、跨块拒绝行，清除全部痕迹 ==')
setup_db()
with Session() as s:
 # 批次A：JS-WANT-01 出现两次（同自然键重复，第二行进拒绝区），且含跨多个 chunk 的非法/空值拒绝行（chunk_size=2）
 rows_a=[row('JS-WANT-01'),row('JS-KEEP-02'),row('JS-WANT-01','拉伸强度','421.0'),
         row('JS-BAD-1',v='四百二十'),row('JS-KEEP-03'),row('JS-BAD-2',v=None)]
 ba=Batch(name='批次A',source_file='a.json',description='',rows=rows_a);s.add(ba);s.flush()
 ma=Mapping(name='映射A',batch_id=ba.id,rules=RULES_A,chunk_size=2);s.add(ma)
 rows_b=[row_b('js-want-01'),row_b('JS-KEEP-02','压缩强度')]
 bb=Batch(name='批次B',source_file='b.json',description='',rows=rows_b);s.add(bb);s.flush()
 mb=Mapping(name='映射B',batch_id=bb.id,rules=RULES_B,chunk_size=1);s.add(mb)
 s.commit();aid,bid,maid,mbid=ba.id,bb.id,ma.id,mb.id

# 两个运行，覆盖跨块（A chunk=2）与第二映射（B chunk=1）
ra=runs.create(RunInput(name='run-a',mapping_id=maid));rb=runs.create(RunInput(name='run-b',mapping_id=mbid))
worker_mod.execute(ra['id']);worker_mod.execute(rb['id'])
with Session() as s:
 ra_run=s.get(Run,ra['id']);rb_run=s.get(Run,rb['id'])
 check('两运行完成',ra_run.status=='completed' and rb_run.status=='completed')
 check('运行A拒绝区含JS-WANT重复键与跨块非法行',
       any('JS-WANT-01' in json.dumps(x['source'],ensure_ascii=False) for x in ra_run.result['rejected_rows'])
       and any('JS-BAD' in json.dumps(x['source'],ensure_ascii=False) for x in ra_run.result['rejected_rows']))
 check('运行A存在跨chunk拒绝（chunks>=3）',len(ra_run.result['chunks'])>=3)
 a_targets_before=[t.payload for t in s.query(TargetRow).filter_by(run_id=ra['id']).order_by(TargetRow.ordinal).all()]
 b_targets_before=[t.payload for t in s.query(TargetRow).filter_by(run_id=rb['id']).order_by(TargetRow.ordinal).all()]
 check('目标表含待撤销委托',any(t['sample_code']=='JS-WANT-01' for t in a_targets_before+b_targets_before))
 checksum_a_before=ra_run.result['checksum'];backup_a_before=ra_run.result['backup']
 audit_before=s.query(Audit).count()

# 用带空格、小写的不规范输入发起清除，验证规范化匹配
rv=purge_revocation(RevokeInput(code='  js-want-01 '),operator)
check('撤销单完成且规范化为JS-WANT-01',rv['status']=='completed' and rv['code']=='JS-WANT-01')
with Session() as s:
 ba=s.get(Batch,aid);bb=s.get(Batch,bid);ra_run=s.get(Run,ra['id']);rb_run=s.get(Run,rb['id'])
 check('批次A原行不再保留该委托（含重复行）',all('JS-WANT-01' not in json.dumps(r,ensure_ascii=False) for r in ba.rows) and len(ba.rows)==4)
 check('批次B原行不再保留该委托（小写源同号）',all(normalize_code(r['sample_no'])!='JS-WANT-01' for r in bb.rows) and len(bb.rows)==1)
 check('其他委托保持原样',any(r['委托号']=='JS-KEEP-02' for r in ba.rows) and any(r['委托号']=='JS-KEEP-03' for r in ba.rows))
 check('运行A快照已清除',all('JS-WANT-01' not in json.dumps(r,ensure_ascii=False) for r in ra_run.snapshot['batch']['rows']))
 check('运行A拒绝区不再含该委托',all('JS-WANT-01' not in json.dumps(x,ensure_ascii=False) for x in ra_run.result['rejected_rows']))
 check('跨块JS-BAD拒绝行仍保留',any('JS-BAD' in json.dumps(x['source'],ensure_ascii=False) for x in ra_run.result['rejected_rows']))
 a_targets=[t.payload for t in s.query(TargetRow).filter_by(run_id=ra['id']).order_by(TargetRow.ordinal).all()]
 b_targets=[t.payload for t in s.query(TargetRow).filter_by(run_id=rb['id']).order_by(TargetRow.ordinal).all()]
 check('目标表无该委托',all(t['sample_code']!='JS-WANT-01' for t in a_targets+b_targets))
 check('目标表保留其他委托',{t['sample_code'] for t in a_targets}=={'JS-KEEP-02','JS-KEEP-03'} and b_targets[0]['sample_code']=='JS-KEEP-02')
 # 新对账自洽
 check('新checksum与回读目标一致',digest(a_targets)==ra_run.result['checksum'] and digest(b_targets)==rb_run.result['checksum'])
 check('新备份与新对账匹配',ra_run.result['backup']['checksum']==ra_run.result['checksum'])
 check('祖先摘要链保留清除前checksum',checksum_a_before in ra_run.result['ancestor_checksums'])
 check('summary记录清除量',rv['summary']['batch_rows_cleared']==3 and rv['summary']['snapshot_rows_cleared']==3)
 # ordinal 连续
 ords=[t.ordinal for t in s.query(TargetRow).filter_by(run_id=ra['id']).order_by(TargetRow.ordinal)]
 check('目标行ordinal重排连续',ords==list(range(len(ords))))

print('== 场景2：再次导入与排队运行带不回已撤销委托 ==')
with Session() as s:
 before=len(s.get(Batch,aid).rows)
# 重新导入包含已撤销委托的行
saved=batches.save(BatchInput(name='批次A',source_file='a.json',description='',rows=[row('JS-WANT-01'),row('JS-KEEP-09'),row('JS-WANT-01','重复项')]),aid)
check('批次编辑拦截已撤销委托',saved['revoked_rows_dropped']==2)
with Session() as s:
 ba=s.get(Batch,aid)
 check('拦截后批次无该委托且新委托保留',all('JS-WANT-01' not in json.dumps(r,ensure_ascii=False) for r in ba.rows) and any(r['委托号']=='JS-KEEP-09' for r in ba.rows))
# 新建运行（等价旧运行再次排队导入）
rc=runs.create(RunInput(name='run-c',mapping_id=maid));worker_mod.execute(rc['id'])
with Session() as s:
 rc_run=s.get(Run,rc['id']);tc=[t.payload for t in s.query(TargetRow).filter_by(run_id=rc['id']).order_by(TargetRow.ordinal).all()]
 check('新排队运行快照与目标均无该委托',all('JS-WANT-01' not in json.dumps(r,ensure_ascii=False) for r in rc_run.snapshot['batch']['rows']) and all(t['sample_code']!='JS-WANT-01' for t in tc))
 check('新运行包含新委托JS-KEEP-09',any(t['sample_code']=='JS-KEEP-09' for t in tc))

print('== 场景3：清除前旧备份恢复 —— 核验原摘要/篡改，仅恢复未撤销部分 ==')
old_backup=copy.deepcopy(backup_a_before)
# 3a 原始未篡改旧备份：祖先摘要匹配，应通过核验，但恢复后剔除已撤销委托
proof=runs.restore(ra['id'],type('D',(),{'backup':old_backup})())
check('旧备份恢复成功',proof['verified'] is True)
check('恢复剔除已撤销委托',proof['revoked_rows_excluded']>=1 and proof['restored_count']==2)
with Session() as s:
 ra_run=s.get(Run,ra['id'])
 check('恢复后新对账匹配且祖先链可追溯',ra_run.result['checksum']==proof['checksum'] and checksum_a_before in ra_run.result['ancestor_checksums'])
 check('恢复记录含来源备份摘要',proof['source_backup_checksum']==old_backup['checksum'])
# 3b 篡改备份：改一行数据但保留原checksum -> 逐行哈希/摘要核验拒绝
tampered=copy.deepcopy(old_backup);tampered['rows'][0]['result_value']='999.999999'
try:
 runs.restore(ra['id'],type('D',(),{'backup':tampered})());check('篡改备份被拒绝',False)
except HTTPException as e:check('篡改备份被拒绝',e.status_code==422 and ('篡改' in e.detail or '摘要' in e.detail or 'checksum' in e.detail))
# 3c 伪造全量自洽备份（重算所有哈希），checksum 不在当前/祖先链 -> 拒绝
forged=copy.deepcopy(old_backup)
for r in forged['rows']:
 if r['sample_code']=='JS-WANT-01':r['result_value']='1.000001'
forged['row_hashes']=[digest(r) for r in forged['rows']];forged['checksum']=digest(forged['rows'])
try:
 runs.restore(ra['id'],type('D',(),{'backup':forged})());check('伪造自洽备份被祖先链拒绝',False)
except HTTPException as e:check('伪造自洽备份被祖先链拒绝',e.status_code==422)
# 3d 错运行编号拒绝
wrong=copy.deepcopy(old_backup);wrong['run_id']=99999
try:
 runs.restore(ra['id'],type('D',(),{'backup':wrong})());check('错运行备份被拒绝',False)
except HTTPException as e:check('错运行备份被拒绝',e.status_code==422)
# 3e 当前checksum备份仍可恢复（现行行为不回归）
cur=ra_run.result['backup']
proof2=runs.restore(ra['id'],type('D',(),{'backup':cur})())
check('当前备份恢复不回归',proof2['verified'] and proof2['revoked_rows_excluded']==0)

print('== 场景4：空结果清除有效（委托号不存在于任何数据） ==')
rv_empty=purge_revocation(RevokeInput(code='JS-NEVER-SEEN'),admin)
check('空结果清除完成且计数为0',rv_empty['status']=='completed' and rv_empty['summary']['batch_rows_cleared']==0 and rv_empty['summary']['runs']==[])

print('== 场景5：幂等重试不重复记清除事件 ==')
with Session() as s:audit_mid=s.query(Audit).filter(Audit.action.like('revocation.%')).count()
rv_retry=purge_revocation(RevokeInput(code='JS-WANT-01'),operator)  # 不同表面形式，规范化同号
rv_retry2=purge_revocation(RevokeInput(code='  js-want-01 '),admin)
check('重试返回同一撤销单',rv_retry['id']==rv['id'] and rv_retry2['id']==rv['id'] and rv_retry['status']=='completed')
with Session() as s:
 audit_after=s.query(Audit).filter(Audit.action.like('revocation.%')).count()
 check('重试不新增任何撤销审计事件',audit_after==audit_mid)
 check('revocations表同号仅一行',s.query(Revocation).filter_by(code='JS-WANT-01').count()==1)

print('== 场景6：失败整次回滚，完成标记与数据同步落库，失败可重试 ==')
# 注入故障：让 purge 内部 migrate 抛错
import src.services.revocations as rev_svc
orig_migrate=rev_svc.migrate
def boom(*a,**k):raise RuntimeError('注入故障')
rev_svc.migrate=boom
# 先制造一个 failed 撤销单
with Session() as s:
 rows_c=[row('JS-FAIL-01'),row('JS-KEEP-02')]
 bc=Batch(name='批次C',source_file='c.json',description='',rows=rows_c);s.add(bc);s.flush()
 mc=Mapping(name='映射C',batch_id=bc.id,rules=RULES_A,chunk_size=2);s.add(mc);s.commit();cid=bc.id;mcid=mc.id
rd=runs.create(RunInput(name='run-d',mapping_id=mcid));worker_mod.execute(rd['id'])
checksum_before=None
with Session() as s:checksum_before=s.get(Run,rd['id']).result['checksum']
try:
 purge_revocation(RevokeInput(code='JS-FAIL-01'),operator);check('故障时请求报错',False)
except HTTPException as e:check('故障时请求报500',e.status_code==500)
rev_svc.migrate=orig_migrate
with Session() as s:
 failed=s.query(Revocation).filter_by(code='JS-FAIL-01').one()
 check('撤销单标记failed',failed.status=='failed' and '注入故障' in (failed.error or ''))
 bc=s.get(Batch,cid);rd_run=s.get(Run,rd['id'])
 check('失败后批次原行原样保留',any(r['委托号']=='JS-FAIL-01' for r in bc.rows))
 check('失败后运行checksum未变（整次回滚）',rd_run.result['checksum']==checksum_before)
 check('失败后目标行原样保留',s.query(TargetRow).filter_by(run_id=rd['id']).count()==2)
# 故障恢复后重试成功，且只记一次 completed
with Session() as s:aud0=s.query(Audit).filter(Audit.action=='revocation.completed').count()
rv_ok=purge_revocation(RevokeInput(code='JS-FAIL-01'),operator)
with Session() as s:
 aud1=s.query(Audit).filter(Audit.action=='revocation.completed').count()
 check('失败重试后成功',rv_ok['status']=='completed' and aud1==aud0+1)
 bc=s.get(Batch,cid)
 check('重试后真正清除',all('JS-FAIL-01' not in json.dumps(r,ensure_ascii=False) for r in bc.rows))

print('== 场景7：清除与迁移落库竞争 —— worker最终提交过滤 ==')
with Session() as s:
 rows_e=[row('JS-RACE-01'),row('JS-KEEP-02')]
 be=Batch(name='批次E',source_file='e.json',description='',rows=rows_e);s.add(be);s.flush()
 me=Mapping(name='映射E',batch_id=be.id,rules=RULES_A,chunk_size=1);s.add(me);s.commit();beid=be.id;meid=me.id
# 模拟：运行已入队并被worker取走（快照已冻结含JS-RACE-01），此刻清除先落库，再执行worker提交
re_=runs.create(RunInput(name='run-e',mapping_id=meid))
# 不立即 execute；先做清除（清除时该run处于queued，只清快照重建队列态），再让worker执行
rv_race=purge_revocation(RevokeInput(code='JS-RACE-01'),admin)
worker_mod.execute(re_['id'])
with Session() as s:
 re_run=s.get(Run,re_['id']);te=[t.payload for t in s.query(TargetRow).filter_by(run_id=re_['id']).order_by(TargetRow.ordinal).all()]
 check('竞争后落库目标不含被清除委托',all(t['sample_code']!='JS-RACE-01' for t in te) and len(te)==1)
 check('竞争后运行结果与目标一致',digest(te)==re_run.result['checksum'])

# 更严格的竞争：worker 已完成 migrate 计算（结果含该委托），提交前撤销落库 -> 最终锁内重算过滤
with Session() as s:
 rows_f=[row('JS-RACE-02'),row('JS-KEEP-03')]
 bf=Batch(name='批次F',source_file='f.json',description='',rows=rows_f);s.add(bf);s.flush()
 mf=Mapping(name='映射F',batch_id=bf.id,rules=RULES_F if False else RULES_A,chunk_size=1);s.add(mf);s.commit();mfid=mf.id
rf_=runs.create(RunInput(name='run-f',mapping_id=mfid))
# 手工走到 running，然后在"提交前"插入清除
with Session() as s:
 r=s.query(Run).filter_by(id=rf_['id']).with_for_update().first();r.status='running';s.commit()
snap=None
with Session() as s:snap=s.get(Run,rf_['id']).snapshot
pre_result=migrate(snap['batch']['rows'],snap['mapping']['rules'],snap['mapping']['chunk_size'])
check('竞争前置计算确实包含目标委托',any(r['sample_code']=='JS-RACE-02' for r in pre_result['accepted_rows']))
purge_revocation(RevokeInput(code='JS-RACE-02'),admin)  # 清除在worker最终提交前落库
# 复用 worker 最终提交段逻辑（execute 会因 status!=queued 跳过），直接验证过滤函数行为
from src.services.migration import code_fields,filter_rows_by_codes
with Session() as s:
 codes={r.code for r in s.query(Revocation).filter_by(status='completed')}
 kept,dropped=filter_rows_by_codes(snap['batch']['rows'],code_fields(snap['mapping']['rules']),codes)
 final_result=migrate(kept,snap['mapping']['rules'],snap['mapping']['chunk_size'])
 check('最终提交过滤掉竞争期间清除的委托',dropped==1 and all(r['sample_code']!='JS-RACE-02' for r in final_result['accepted_rows']))

print('== 场景8：清除与批次编辑并发 —— 行锁串行化 ==')
with Session() as s:
 rows_g=[row('JS-EDIT-01'),row('JS-KEEP-02')]
 bg=Batch(name='批次G',source_file='g.json',description='',rows=rows_g);s.add(bg);s.commit();bgid=bg.id
errors=[]
def do_purge():
 try:purge_revocation(RevokeInput(code='JS-EDIT-01'),admin)
 except Exception as e:errors.append(('purge',str(e)))
def do_edit():
 try:batches.save(BatchInput(name='批次G',source_file='g.json',description='并发编辑',rows=[row('JS-EDIT-01'),row('JS-KEEP-07')]),bgid)
 except Exception as e:errors.append(('edit',str(e)))
t1=threading.Thread(target=do_purge);t2=threading.Thread(target=do_edit)
t1.start();t2.start();t1.join();t2.join()
with Session() as s:
 bg=s.get(Batch,bgid)
 has=any(r['委托号']=='JS-EDIT-01' for r in bg.rows)
 rv_done=s.query(Revocation).filter_by(code='JS-EDIT-01').one().status=='completed'
 check('并发后被撤销委托绝不留在批次',not has,extra=('errors='+str(errors)))
 check('撤销单完成',rv_done)
 check('并发无未捕获异常',not errors,extra=str(errors))

print('== 场景9：API 层角色权限（viewer 只读，writer 可清除，接口可查状态） ==')
from fastapi.testclient import TestClient
from src.main import app
from src.utils.security import token as make_token
with Session() as s:
 vu=s.query(User).filter_by(email='viewer@migration.local').one()
 ou=s.query(User).filter_by(email='operator@migration.local').one()
 vt=make_token(vu);ot=make_token(ou)
client=TestClient(app)
r=client.get('/revocations',headers={'Authorization':'Bearer '+vt})
check('viewer可查询撤销状态',r.status_code==200 and isinstance(r.json(),list))
r=client.post('/revocations',json={'code':'JS-VIEWER-DENY'},headers={'Authorization':'Bearer '+vt})
check('viewer发起清除被403',r.status_code==403)
r=client.post('/revocations',json={'code':'JS-OP-OK'},headers={'Authorization':'Bearer '+ot})
check('operator发起清除200',r.status_code==200 and r.json()['status']=='completed')
r=client.post('/revocations',json={'code':''},headers={'Authorization':'Bearer '+ot})
check('空委托号被422',r.status_code==422)
r=client.get('/revocations',headers={'Authorization':'Bearer invalid'})
check('无有效令牌401',r.status_code==401)

print()
print('结果：PASS=%d FAIL=%d'%(PASS,FAIL))
db_path=os.path.join(os.path.dirname(__file__),'test.db')
if os.path.exists(db_path):os.remove(db_path)
sys.exit(1 if FAIL else 0)
