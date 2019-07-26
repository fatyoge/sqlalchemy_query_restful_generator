from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
import logging
from pyhive import hive
from collections import OrderedDict 
import json
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
from flask_restful import marshal, fields
from sqlalchemy.sql import sqltypes
import re
from utils import SQLFormator
from flask_restful import fields

class Connector():
    #url = 'driver://username:password@host:port/database'
    def __init__(self, name):
        self.engine = {}
        self.connect_url = {}
        self.table_obj = {}
        self.server_name = name
        self.table_whitelist = None
        self.table_blacklist = None
        self.default_schema = 'default'

    def set_addr(self, url):
        for k in url:
            self.connect_url[k]=url[k]
            if k in ['schema','database']:
                self.default_schema = url[k]
        
    def set_permission(self, table_whitelist=None, table_blacklist=None):
        self.table_whitelist = table_whitelist
        self.table_blacklist = table_blacklist

    def check_permission(self, table_name):
        if self.table_blacklist is not None and table_name in self.table_blacklist:
            return False
        elif self.table_whitelist is not None and table_name not in self.table_whitelist:
            return False
        else:
            return True
    
    def _create_engine(self, schema=None, url=None):
        if url is None:
            url = self._generate_url(schema)
        engine = create_engine(url)
        logging.info('Create engine success.')
        logging.info(engine)
        return engine

    def get_engine(self, schema=None, url=None):
        if schema in self.engine:
            return self.engine[schema]
        self.engine[schema] = self._create_engine(schema=schema,url=url)
        return self.engine[schema]
    
    def get_table(self, table_name, dbschema=None):
        logging.info('Start to create orm object: {}'.format(table_name))
        #print('Start to create orm object: {}'.format(table_name))
        table_params = table_name.split('.')
        table_name = table_params[-1]

        if table_name in self.table_obj:
            return self.table_obj[table_name]

        if dbschema is None:
            dbschema = table_params[0] if len(table_params) == 2 else self.default_schema
        #logging.info('dbschema: {}'.format(dbschema))
        table_orm = Table(table_name, MetaData(bind=self.get_engine(dbschema)), autoload=True)
        self.table_obj[table_name] = table_orm
        return table_orm
    
    def get_table_schema(self, table):
        if not self.check_permission(table):
            logging.warning('No permission to access the table')
            return json.dumps({'message': 'No permission to access the table'})
        if isinstance(table, str):
            table = self.get_table(table)
        #schema = OrderedDict()
        schema = []
        for col in table.c:
            schema.append(col.name, col.type, col.nullable)
        return schema
    
    def get_table_list(self):
        return self.get_engine().table_names()

    def query_table(self, table, fields=None, whereclause=None, order_by=None, offset=None, group_by=None, limit=10, page=None, **kwargs):
        if not self.check_permission(table):
            logging.warning('No permission to access the table')
            return json.dumps({'message': 'No permission to access the table'})
        logging.info('start to query_table： {}'.format(table))
        query_args = OrderedDict()
        # Get table ORM object
        if isinstance(table, str):
            table = self.get_table(table)
        query_args['from_obj'] = table

        query_args['columns'] = SQLFormator.selectTransform(fields, table)

        if group_by is not None:
            query_args['group_by'] = group_by.split(',')

        if order_by is not None:
            query_args['order_by'] = SQLFormator.orderbyTransform(order_by)
                
        if whereclause is not None:
            query_args['whereclause'] = SQLFormator.whereTransform(whereclause, table)
        
        query_args['limit'] = limit if limit is not None else 1

        if page is not None:
        #    query_args['offset'] = query_args['limit'] * (page - 1)
            logging.warning('Page did not support on hive, so _p param would be ignored.')

        #print('=======================query_args=========================')
        #print(query_args)
        #return select(**query_args).scalar()
        query = select(**query_args)

        resource_type = SQLFormator.getResourceType(query, table)

        result = select(**query_args).execute().fetchall()
        logging.info(result)
        logging.info(resource_type)
        return json.dumps(marshal(result, resource_type))
    
class HiveSqlaConnector(Connector):
    def __init__(self, name):
        super().__init__(name)
        self.connect_url['driver'] = 'hive'
        pass
    
    def set_addr(self, url):
        super().set_addr(url)
        self.metastore = url["metastore"]
    
    def get_engine(self, schema=None, url=None):
        if len(self.engine) == 0:
            self.engine["metastore"] = self._create_engine(url=self.metastore)
        return super().get_engine(schema=schema, url=url)
    
    def get_table_list(self, schema=None, limit=10):
        tbls = self.get_table('TBLS', "metastore")
        dbs  = self.get_table('DBS', "metastore")

        resource_type = OrderedDict()
        resource_type['dbschema'] = fields.String
        resource_type['owner'] = fields.String
        resource_type['table_name'] = fields.String
        resource_type['type'] = fields.String
        #query_args['from_obj'] = [, self.get_table('', "metastore")]
        query = select([dbs.c.NAME.label('dbschema'), 
                        dbs.c.OWNER_NAME.label('owner'), 
                        tbls.c.TBL_NAME.label('table_name'), 
                        tbls.c.TBL_TYPE.label('type')])\
                .where(dbs.c.DB_ID == tbls.c.DB_ID)
        if schema is not None:
            query = query.where(dbs.c.NAME == schema)

        query = query.limit(limit)
        result = query.execute().fetchall()
        return json.dumps(marshal(result, resource_type))

    def get_table_schema(self, table):
        tbls = self.get_table('TBLS', "metastore")
        cols  = self.get_table('COLUMNS_V2', "metastore")

        resource_type = OrderedDict()
        resource_type['COLUMN_NAME'] = fields.String
        resource_type['TYPE'] = fields.String
        resource_type['COMMENT'] = fields.String
        #query_args['from_obj'] = [, self.get_table('', "metastore")]
        query = select([cols.c.COLUMN_NAME, 
                        cols.c.TYPE_NAME.label('TYPE'),
                        cols.c.COMMENT])\
                .where(tbls.c.TBL_ID == cols.c.CD_ID)\
                .where(tbls.c.TBL_NAME == table)
        #if schema is not None:
        #    query = query.where(dbs.c.NAME == schema)
        query = query.order_by(cols.c.INTEGER_IDX)
        result = query.execute().fetchall()
        return json.dumps(marshal(result, resource_type))
    
    def _generate_url(self, schema=None):
        if schema is None:
            schema = self.default_schema
        url = '{}://{}@{}:{}/{}?{}'.format(
                self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['host']
                , self.connect_url['port']
                , schema
                , self.connect_url['param']
                )
        return url

    

#class HiveDBApiConnector(Connector):
#    def __init__(self, name):
#        super().__init__(name)
#        pass
#    
#    def _create_engine(self, schema):
#        engine = hive.connect(
#                  host=self.connect_url['host']
#                , port=self.connect_url['port']
#                , username=self.connect_url['username']
#                , database=schema
#                , auth=self.connect_url['auth']
#            ).cursor()
#        logging.info('HiveDBApiConnector create engine success.')
#        logging.info(engine)
#        return engine

class PrestoConnector(Connector):
    def __init__(self, name):
        super().__init__(name)
        self.connect_url['driver'] = 'presto'
        pass
    
    def _generate_url(self, schema=None):
        if schema is None:
            schema = self.default_schema
        url = '{}://{}@{}:{}/{}/{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['host']
                , self.connect_url['port']
                , self.connect_url['param']
                , schema
                )
        return url

class MysqlConnector(Connector):
    def __init__(self, name):
        super().__init__(name)
        #self.connect_url['driver'] = 'mysql'
        self.connect_url['driver'] = 'mysql+pymysql'
        pass
    
    def _generate_url(self, schema=None):
        if schema is None:
            schema = self.default_schema
        # url: mysql+pymysql://scott:tiger@localhost:3306/foo
        port = 3306 if 'port' not in self.connect_url else self.connect_url['port']
        url = '{}://{}:{}@{}:{}/{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['password']
                , self.connect_url['host']
                , port
                , schema
                )
        return url

class PostgresConnector(Connector):
    def __init__(self, name):
        super().__init__(name)
        self.connect_url['driver'] = 'postgresql'
        pass
    
    def _generate_url(self, schema=None):
        if schema is None:
            schema = self.default_schema
        # url: postgresql://scott:tiger@localhost:5432/mydatabase
        # logging.info(self.connect_url)
        port = 5432 if 'port' not in self.connect_url else self.connect_url['port']
        url = '{}://{}:{}@{}:{}/{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['password']
                , self.connect_url['host']
                , port
                , schema
                )
        return url

class OracleConnector(Connector):
    def __init__(self, name):
        super().__init__(name)
        self.connect_url['driver'] = 'oracle'
        pass
    
    def _generate_url(self, schema=None):
        if schema is None:
            schema = self.default_schema
        # url: oracle://scott:tiger@127.0.0.1:1521/sidname
        port = 1521 if 'port' not in self.connect_url else self.connect_url['port']
        url = '{}://{}:{}@{}:{}/{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['password']
                , self.connect_url['host']
                , port
                , schema
                )
        return url

class MSSqlConnector(Connector):
    def __init__(self, name):
        super().__init__(name)
        self.connect_url['driver'] = 'mssql+pymssql'
        pass
    
    def _generate_url(self, schema=None):
        if schema is None:
            schema = self.default_schema
        # url: mssql+pymssql://scott:tiger@hostname:port/dbname
        port = 1433 if 'port' not in self.connect_url else self.connect_url['port']
        url = '{}://{}:{}@{}:{}/{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['password']
                , self.connect_url['host']
                , port
                , schema
                )
        return url

def singleton(cls):
    instance = cls()
    instance.__call__ = lambda: instance
    return instance

@singleton
class ConnectorFactory:
    def __init__(self):
        self.connector_list = {}
        self.connectorFactory = {
            'PrestoConnector': PrestoConnector,
            'HiveSqlaConnector': HiveSqlaConnector,
            'MysqlConnector': MysqlConnector,
            'PostgresConnector': PostgresConnector,
            'OracleConnector': OracleConnector,
            'MSSqlConnector': MSSqlConnector,
#            'HiveDBApiConnector': HiveDBApiConnector
        }
        self.curr_connector = None

    def get_or_createConnector(self, url=None, server_name=None
            , connect_type=None, table_whitelist=None, table_blacklist=None):
        if url is None:
            logging.error('Please input the right host addr.')
            return 

        if server_name in self.connector_list:
            return self.connector_list[server_name]

        connectorIns = self.connectorFactory[connect_type](server_name)
        connectorIns.set_addr(url)
        connectorIns.set_permission(*[table_whitelist, table_blacklist])
        #connectorIns.get_engine()

        self.connector_list[server_name] = connectorIns
        self.curr_connector = connectorIns
        return connectorIns

    def connection_test(self):
        print (self.curr_connector.get_engine().table_names())
