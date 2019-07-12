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
    
    def get_engine(self, schema='default'):
        if schema in self.engine:
            return self.engine[schema]
        self.engine[schema] = self._create_engine(schema)
        return self.engine[schema]
    
    def get_table(self, table_name):
        logging.info('Start to create orm object: {}'.format(table_name))
        #print('Start to create orm object: {}'.format(table_name))
        table_params = table_name.split('.')
        table_name = table_params[-1]
        dbschema = table_params[0] if len(table_params) == 2 else self.default_schema
        #logging.info('dbschema: {}'.format(dbschema))
        table_orm = Table(table_name, MetaData(bind=self.get_engine(dbschema)), autoload=True)
        if table_name not in self.table_obj:
            self.table_obj[table_name] = table_orm
        return table_orm
    
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
    
    def _create_engine(self, schema):
        url = '{}://{}@{}:{}/{}?{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['host']
                , self.connect_url['port']
                , schema
                , self.connect_url['param']
                )
        engine = create_engine(url)
        logging.info('HiveSqlaConnector create engine success.')
        logging.info(engine)
        return engine

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
    
    def _create_engine(self, schema):
        url = '{}://{}@{}:{}/{}/{}'.format(
                  self.connect_url['driver']
                , self.connect_url['username']
                , self.connect_url['host']
                , self.connect_url['port']
                , self.connect_url['param']
                , schema
                )
        engine = create_engine(url)
        logging.info('PrestoConnector create engine success.')
        logging.info(engine)
        return engine

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
        if self.curr_connector is None:
            self.curr_connector = connectorIns
        return connectorIns
