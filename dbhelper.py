from contextlib import contextmanager
from math import exp
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
import functools
from typing import Iterable
from sqlalchemy.sql import func

# 数据库连接配置
engines = None

def init_engines():
    """初始化数据库连接"""
    engines = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/test?charset=utf8',
                                   max_overflow=-1,
                                   pool_recycle=1000,
                                   echo=False)
    print(f'enginetest:{engines}')
    return engines
    
    

# 初始化所有数据库的连接，后续如果新增数据库访问，在MYSQL里面直接加入数据库配置即可
# init_engines()

def get_session(db):
    """获取session"""
    session_factory = sessionmaker(bind=init_engines())
    return scoped_session(session_factory)()


@contextmanager
def Db_session(db='test', commit=True):
    """db session封装.

    :params db:数据库名称
    :params commit:进行数据库操作后是否进行commit操作的标志
                   True：commit, False:不commit
    """
    session = get_session(db)
    try:
        yield session
        if commit:
            session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        if session:
            session.close()

def class_dbsession(commit=True):
    """用于BaseModel中进行数据库操作前获取dbsession操作.

    :param commit:进行数据库操作后是否进行commit操作的标志，True：commit, False:不commit
    """
    def wrapper(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            # cls为对象或类
            cls = args[0]
            # 实际传入的参数
            new_args = args[1:]
            with Db_session(cls._db_name, commit) as session:
                return func(cls, session, *new_args, **kwargs)
        return inner
    return wrapper

class BaseModel(object):
    u"""基础模型."""

    _db_name = 'test'

    @class_dbsession(True)
    def add(self, session):
        u"""增.

        eg: a = MerchantBillDetail(id=1)
            a.add()
        """
        session.add(self)

    @classmethod
    @class_dbsession(True)
    def batch_add(cls, session, objs):
        """批量增加.

        eg: a = [MerchantBillDetail(id=1), MerchantBillDetail(id=2)]
            MerchantBillDetail.batch_add(a)
        """
        return session.add_all(objs)

    @classmethod
    @class_dbsession(True)
    def delete(cls, session, where_conds = None):
        u"""删.

        eg: BaseModel.delete([BaseModel.a>1, BaseModel.b==2])
        """
        if where_conds is None:
            where_conds = []
        session.query(cls).filter(*where_conds).delete(
            synchronize_session='fetch')

    @classmethod
    @class_dbsession(True)
    def update(cls, session, update_dict, where_conds = None):
        u"""更新.

        eg: BaseModel.update({'name': 'jack'}, [BaseModel.id>=1])
        """
        if where_conds is None:
            where_conds = []
        return session.query(cls).filter(*where_conds).update(
            update_dict,
            synchronize_session='fetch')

    @classmethod
    @class_dbsession(True)
    def join(cls, session, other_model, join_condition):
        u"""
        两表连接查询
        """
        return session.query(cls, other_model).join(
            other_model, *join_condition)

    @classmethod
    @class_dbsession(False)
    def query(cls, session, params, **where_conds):
        # sourcery skip: raise-specific-error
        u"""查询.

        eg: BaseModel.query([BaseModel.id, BaseModel.name],
                filter=[BaseModel.id>=1],
                group_by=[BaseModel.id, BaseModel.name]
                order_by=BaseModel.id.desc(), limit=10, offset=0)
        """
        if not where_conds and not set(where_conds.keys()).issubset({'filter', 'group_by', 'order_by', 'limit', 'offset'}):
            raise Exception('input para error!')
        cfilter = where_conds.pop('filter', None)
        group_para = where_conds.pop('group_by', None)
        order_para = where_conds.pop('order_by', None)
        limit = where_conds.pop('limit', None)
        offset = where_conds.pop('offset', None)
        query_first = where_conds.get('query_first', False)

        if not isinstance(params, Iterable):
            params = [params]
        squery = session.query(*params)
        if cfilter is not None:
            # print(f'filter:{cfilter}')
            # for key, value in kwargs.items():
            #     print("The value of {} is {}".format(key, value))
            squery = squery.filter(*cfilter)
            # squery = squery.filter_by(**cfilter)
        if group_para is not None:
            squery = squery.group_by(*group_para)
        if order_para is not None:
            squery = squery.order_by(order_para)
        if limit is not None:
            squery = squery.limit(limit)
        if offset is not None:
            squery = squery.offset(offset)
        if query_first:
            return squery.first()
        return squery.all()

    @classmethod
    @class_dbsession(False)
    def __aggregate(cls, session, aggr_fun, params, where_conds = None):
        u"""对参数进行聚合函数(sum, avg, max, min)计算.

        BaseModel.__aggregate(func.sum,
                            [BaseModel.id, BaseModel.num], [BaseModel.id==1])
        """
        if where_conds is None:
            where_conds = []
        if not isinstance(params, Iterable):
            params = [params]
        aggr_list = [aggr_fun(param) for param in params]
        re = session.query(*aggr_list).filter(*where_conds).one()
        if len(re) == 1:
            return re[0] or 0
        return [i or 0 for i in re]

    @classmethod
    def sum(cls, params, where_conds = None):
        u"""求和.

        eg: BaseModel.sum([BaseModel.id], [BaseModel.id==1])
        """
        if where_conds is None:
            where_conds = []
        return cls.__aggregate(func.sum, params, where_conds)

    @classmethod
    def max(cls, params, where_conds = None):
        u"""求最大值.

        eg: BaseModel.max([BaseModel.num], [BaseModel.id==2])
        """
        if where_conds is None:
            where_conds = []
        return cls.__aggregate(func.max, params, where_conds)

    @classmethod
    @class_dbsession(False)
    def count(cls, session, params, where_conds = None, distinct=False):
        u"""计数.

        eg: BaseModel.count([BaseModel.id, BaseModel.XXX], [BaseModel.id==2])
            BaseModel.count(BaseModel.id, [BaseModel.id==2], True)
        """
        if where_conds is None:
            where_conds = []
        if distinct:
            if isinstance(params, Iterable) and len(params) >= 2:
                re = session.query(func.count(
                    func.distinct(func.concat(*params))))\
                    .filter(*where_conds).one()[0]
            elif isinstance(params, Iterable):
                qp = params[0]
                re = session.query(func.count(func.distinct(qp))).filter(
                    *where_conds).one()[0]
            else:
                re = session.query(func.count(func.distinct(
                    params))).filter(*where_conds).one()[0]
        else:
            if not isinstance(params, Iterable):
                params = [params]
            re = session.query(*params).filter(*where_conds).count()
        return re

    @classmethod
    @class_dbsession(False)
    def simple_paging_query(cls, session, params, where_conds, page_size=100):
        """简单分页查询
        """
        total_count = cls.count([cls.id], where_conds)
        rv = []
        for offset in range(0, total_count, page_size):
            rv.extend(cls.query(
                params, filter=where_conds, offset=offset, limit=page_size
            ))
        return rv

    @classmethod
    def execute(cls, sql_str):
        with Db_session(cls._db_name, commit=True) as session:
            return session.execute(sql_str)
