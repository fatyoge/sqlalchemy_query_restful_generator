# sqlalchemy_query_restful_generator
Generate REST APIs for sqlalchemy connections by easy setting. Similar as (xmysql)[https://github.com/o1lab/xmysql].

# Usage

1. Edit config.py, write down your db host. For now we only test presto & hive connection, so pick one.

2. Start the server: python flask_demo.py <server_name>, which is in config.py

3. Try curl http://localhost:5000/api/payments?_fields=customerNumber,checkNumber

# API overview

|HTTP TYPE|API URL|COMMENT|
|---|---|---|
|GET|/API/TABLE_NAME|Query the table if not in the blacklist|

### Column filtering / Fields
```
/api/payments?_fields=customerNumber,checkNumber
```
### Row filtering / Where
```
/api/payments?_where=(checkNumber,eq,JM555205)~or((amount,gt,200)~and(amount,lt,2000))
```
For now we support these comparison operators 
```
eq      -   '='         -  (colName,eq,colValue)
ne      -   '!='        -  (colName,ne,colValue)
gt      -   '>'         -  (colName,gt,colValue)
gte     -   '>='        -  (colName,gte,colValue)
lt      -   '<'         -  (colName,lt,colValue)
lte     -   '<='        -  (colName,lte,colValue)
```
### Order by / Sorting
```
/api/payments?_sort=column1,-column2
```
eg: sorts ascending by column1 and descending by column2

### Group By
```
/api/offices?_groupby=country
```
### Limit
```
/api/payments?_size=50
```
### Arrg function
```
/api/payments?_fields=date,sum(amt),count(*)&_where=(amt,gt,199)&_groupby=date&_sort=-date&_size=10
```
eg: select date,sum(amt),count(*) from payments where amt > 199 group by date order by date desc limit 10;
