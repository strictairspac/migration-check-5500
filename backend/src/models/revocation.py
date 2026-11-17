from sqlalchemy import Column,Integer,String,JSON,DateTime,func,Text
from src.config.database import Base
class Revocation(Base):
 __tablename__='revocations'
 id=Column(Integer,primary_key=True)
 # 规范化委托号，唯一约束保证同号重复请求是重试而不是第二张撤销单
 code=Column(String(160),unique=True,nullable=False)
 requested_code=Column(String(160),nullable=False)
 status=Column(String(20),nullable=False,default='processing')
 actor=Column(String(120),nullable=False,default='')
 summary=Column(JSON,nullable=True)
 error=Column(String(1000),nullable=True)
 created_at=Column(DateTime,server_default=func.now())
 updated_at=Column(DateTime,server_default=func.now(),onupdate=func.now())
