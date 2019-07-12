import re
from sqlalchemy import *
from sqlalchemy.sql import sqltypes
from sqlalchemy import desc, asc 
from flask_restful import fields
from collections import OrderedDict 
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def singleton(cls):
    instance = cls()
    instance.__call__ = lambda: instance
    return instance

@singleton
class SQLFormator:
    string_type = [
        sqltypes.String,
        sqltypes.DateTime,
        sqltypes.Date,
    ]
    operator_map = {
        'eq':'=',
        'ne':'!=',
        'gt':'>',
        'gte':'>=',
        'lt':'<',
        'lte':'<=',
    }
    func_map = {
        'sum' : func.sum,
        'count': func.count,
        'max' : func.max,
        'min' : func.min,
        'avg' : func.avg,
        'median' : func.median,
    }
    type_map = {
        str : fields.String,
        int : fields.Integer,
        float : fields.Float,
    }
    @classmethod
    def _whereSingleTransform(cls, txt, table):
        ops = ['~or','~and','~xor']
        if str(txt).strip() in ops:
            return " {} ".format(str(txt).strip().replace('~',''))

        cond = txt.split(',')
        col = cond[0]
        oper = cls.operator_map[cond[1]]
        value = cond[2]
        if col in table.alias().columns and type(table.alias().columns[col].type) in cls.string_type:
            value = "'{}'".format(value)
        return "{} {} {}".format(cond[0], oper, value)
    
    @classmethod
    def whereTransform(cls, txt, table):
        wheres = re.findall(r'[^()]+',txt)
        for subwhere in wheres:
            tmp = cls._whereSingleTransform(subwhere, table)
            txt = str(txt).replace(subwhere,tmp,1)
        return txt

    @classmethod
    def selectTransform(cls, fields, table):
        cols = []
        #resource_type = OrderedDict()
        if fields is None:
            cols = table.c.values()
        else:
            fields = fields.split(',')
            for field in fields:
                col = cls._selectSingleTransform(field, table)
                cols.append(col)
        #for col in cols:
        #    resource_type[col.name] = cls.type_map[col.type.python_type]
        #return cols, resource_type
        return cols
    
    @classmethod
    def _selectSingleTransform(cls, field, table):
        regs = re.findall(r'[^()]+',field)
        if len(regs) == 1:
            return table.c[field]
        elif len(regs) == 2 and regs[0] in cls.func_map:
            col_name = regs[1]
            col = cls.func_map[regs[0]](table.columns[col_name] if col_name in table.columns else col_name)
            #col.name = '{}_{}'.format(regs[0], regs[1]).replace('*','table')
            return col
    
    @classmethod
    def orderbyTransform(cls, order_by):
        return [desc(x[1:]) if x[0]=='-' else asc(x) for x in order_by.split(',')]


    @classmethod
    def getResourceType(cls, query, table):
        resource_type = OrderedDict()
        for col in query.c:
            #logging.info('{} in table.c: {}'.format(col, str(col) in table.c))
            #if str(col) in table.c:
            #    resource_type[str(col)] = cls.type_map[table.c[str(col)].type.python_type]
            #else:
            #    resource_type[str(col)] = cls.type_map[col.type.python_type]
            resource_type[str(col)] = cls.type_map[table.c[str(col)].type.python_type] if str(col) in table.c else cls.type_map[col.type.python_type]
        return resource_type
