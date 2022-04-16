from typing import Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, BigInteger, DateTime, Numeric, Boolean,Text, and_, or_
# from dbhelper import BaseModel
from sqlalchemy.dialects.mysql import CHAR, VARCHAR
from datetime import datetime
from dbhelper import BaseModel, init_engines  
# for first time run, init create table
Base = declarative_base()
    
class Order(Base, BaseModel):
    __tablename__ = 'orders'
    order_id = Column(BigInteger, primary_key=True)
    member_id = Column(BigInteger)
    contact_person = Column(String(128))
    address = Column(Text)
    code = Column(CHAR(6))
    phone = Column(String(16))
    add_time = Column(DateTime(), default=datetime.now)
    total = Column(Numeric(10,2))
    status = Column(Boolean(False))
    update_time = Column(DateTime(), default=datetime.now)
    
class OrderDetail(Base, BaseModel):
    __tablename__ = 'order_detail'
    detail_id = Column(BigInteger, primary_key=True)
    orderid = Column(BigInteger)
    goods_id = Column(BigInteger)
    goods_name = Column(String(128))
    price = Column(Numeric(6,2))
    amount = Column(BigInteger)
    goods_desc = Column(Text)
    goods_comment = Column(Text)
    order_createtime = Column(DateTime(), default=datetime.now)
    order_updatetime = Column(DateTime(), default=datetime.now)

if __name__=="__main__":
    # first time need run
    # Base.metadata.create_all(bind=init_engines())

    # # 单表精确查询
    # print(OrderDetail.query([OrderDetail.goods_name], filter=[OrderDetail.detail_id==1]))
    
    # # 单表模糊查询
    # # Order.query([Order.contact_person], filter=[Order.contact_person.like('%will%')])
    
    # # 单表多条件查询
    # OrderDetail.query([OrderDetail.detail_id], filter=[OrderDetail.detail_id==1, OrderDetail.goods_name=='banana'])
    
    for i in range(100):
        #新增父表
        order = Order(member_id=i+1, contact_person=f'lucy{i}', code=f'1234{i}', phone=f'15810098146{i}', total=100, status=1)
        order.add()
    
        #新增子表
        order_detail = OrderDetail(orderid=i+1, goods_id=i+1, goods_name=f'apple{i}', price=10+i, amount=1+i, goods_desc=f'apple{i}', goods_comment=f'apple{i} is nice')
        order_detail.add()
    
     # # 删除
    # OrderDetail.delete([OrderDetail.detail_id == 1])

    # # 修改
    # OrderDetail.update({'goods_desc': 'apple is cheaper than other fruit','goods_comment':'next time i will buy it again'}, [OrderDetail.detail_id == 1])

    