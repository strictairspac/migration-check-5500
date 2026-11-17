ROLES=('admin','operator','viewer')
STATUSES=('queued','running','completed','failed','cancelled')
TYPES={'sample_code':'text','test_name':'text','result_value':'decimal','unit':'text','measured_at':'datetime'}
# 委托撤销清除生命周期：processing 清除中（未提交不生效）/ completed 已生效 / failed 整次回滚
REVOCATION_STATUSES=('processing','completed','failed')
