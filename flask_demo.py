from flask import Flask, request, Blueprint
from flask_restful import Resource, Api ,reqparse, fields, marshal_with, marshal
from db_connector import *
import json
from flask_restful import reqparse
from config import SETTING
import sys
#import ptvsd
#ptvsd.enable_attach(address = ('0.0.0.0', 5678))
#ptvsd.wait_for_attach()

app = Flask(__name__)
api_bp = Blueprint('api', __name__)
api = Api(api_bp)

todos = {}
class QueryTable(Resource):
    #def __init__(self, )
    def get(self, table_name):
        db_conn = ConnectorFactory.curr_connector
        parser = reqparse.RequestParser()
        parser.add_argument('_size', type=int, dest='limit')
        parser.add_argument('_fields', type=str, dest='fields')
        parser.add_argument('_where', type=str, dest='whereclause')
        parser.add_argument('_sort', type=str, dest='order_by')
        parser.add_argument('_groupby', type=str, dest='group_by')
        parser.add_argument('_p', type=int, dest='page')
        args = parser.parse_args()
        #result = db_conn.query_table(table_name, **args)
        #print(result)
        #return json.dumps(marshal(db_conn.query_table(table_name, **args), resource_fields))
        return db_conn.query_table(table_name, **args)
    
class QueryStruct(Resource):
    #def __init__(self, )
    def get(self, table_name):
        db_conn = ConnectorFactory.curr_connector
        parser = reqparse.RequestParser()
        #parser.add_argument('_db', type=str, dest='schema')
        #args = parser.parse_args()
        return db_conn.get_table_schema(table_name)

class QueryTableList(Resource):
    def get(self):
        db_conn = ConnectorFactory.curr_connector
        parser = reqparse.RequestParser()
        parser.add_argument('_db', type=str, dest='schema')
        parser.add_argument('_size', type=int, dest='limit')
        args = parser.parse_args()
        return db_conn.get_table_list(**args)

    
api.add_resource(QueryTable, '/api/<string:table_name>/')
api.add_resource(QueryStruct, '/struct/<string:table_name>/')
api.add_resource(QueryTableList, '/list/')
#api.add_resource(QueryTableList, '/list/<string:dbschema>/')


#api.add_resource(Query, '/api/')
app.register_blueprint(api_bp)
 

if __name__ == '__main__':
    if len(sys.argv) == 1:
        logging.error("Usage: python flask_rest_server.py [server_name]")
        sys.exit(1)
    elif sys.argv[1] not in SETTING.server_list:
        logging.error("server {} not in SETTING config, please give a check.".format(sys.argv[1]))
        sys.exit(1)
        
    cf = ConnectorFactory
    cf.get_or_createConnector(server_name=sys.argv[1], **SETTING.server_list[sys.argv[1]])
    #app.run(host='0.0.0.0')
    app.run(debug=True,host='0.0.0.0')
