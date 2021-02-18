## PiSeduce Worker

### Start the API
```
python3 worker_api.py
```

### Start the action executor
```
python3 worker_exec.py
```

### API description

#### User API

#### Admin API

### Read SQLite database
* You must install the client `sqlite3`
```
sqlite3
.open test.db
.tables
.schema <table_name>
select * from <table_name>
delete from <table_name>
```
