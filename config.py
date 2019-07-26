class SETTING:
    server_list = {
        "presto": {
            "connect_type": "PrestoConnector",
            "url": {
                "username": "hive"
                ,"host": ""
                ,"port": 3600
                ,"param" : "hive"
                ,"schema": "default"
                ,"metastore": "mysql+pymysql://hive:hive@/hive"
            },
            "table_whitelist":  [],
            "table_blacklist":  [],
        },
        "hive": {
            "connect_type": "HiveSqlaConnector",
            "url": {
                "username": "yarn"
                ,"host": ""
                ,"port": 10000
                ,"schema": "default"
                ,"param" : "auth=NONE"
                ,"metastore": "mysql+pymysql://hive:hive@/hive"
            },
            "table_whitelist":  [],
            "table_blacklist":  [],
        },
    }
