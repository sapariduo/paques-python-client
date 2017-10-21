# paques-python-client
python client for Paques Big Data

query data model: 
```python
{'data': {'user': 'administrator', 'query': 'search from file cars.csv into cars1| search from file cars.csv into cars2'}, 'event': 'query'}
```
usage examples:  
create connection parameters:
```python
conn = PaquesRequest(host='192.168.0.18', port=8111, user='administrator')
```

prepare query execution
```python
query_preps = PaquesQuery(conn, pql=query)
```

send query command, it will return  
query execution id,   
url coordinator node
```python
inquiry = query_prep.load()
```
to get inquiry results: 
```python
inquiry.id
inquiry.node_url
```
to get query results:
```python
getresult = query_prep.execute()
```
to get query results dataset:  
tables list:
```python
getresult.tables
```
datasets:
```python
getresult.datasets
```
