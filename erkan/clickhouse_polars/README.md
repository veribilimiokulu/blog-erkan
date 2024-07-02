## Create directory
```commandline
mkdir chpolars_play
cd chpolars_play/
```
## Clickhouse Docker
- docker-compose.yaml
```yaml
version: '3.8'
services:
  clickhouse:
    container_name: clickhouse
    image: clickhouse/clickhouse-server:24.5.1
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse/
    environment:
      - CLICKHOUSE_DB=clickhouse_db
      - CLICKHOUSE_USER=clickhouse_user
      - CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1
      - CLICKHOUSE_PASSWORD=Ankara06
volumes:
  clickhouse_data:
```

### Compose up
```commandline
docker-compose up -d
```

### Connect clickhouse and client
```commandline
docker exec -it clickhouse bash

root@9dfe07f25e30:/# clickhouse-client --host localhost --port 9000 --user clickhouse_user --password Ankara06
```

#### show dbs
```commandline
9dfe07f25e30 :) show databases;

SHOW DATABASES

Query id: 5d620290-b6e3-420c-b59a-f2ff879e09d3

   ┌─name───────────────┐
1. │ INFORMATION_SCHEMA │
2. │ clickhouse_db      │
3. │ default            │
4. │ information_schema │
5. │ system             │
   └────────────────────┘

5 rows in set. Elapsed: 0.004 sec.
```
#### Create a table and insert records
```commandline
CREATE TABLE clickhouse_db.crypto_prices (
    trade_date Date,
    crypto_name LowCardinality(String),
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO clickhouse_db.crypto_prices SELECT * FROM 
s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');
```

#### Row count of table
```commandline
SELECT COUNT() from clickhouse_db.crypto_prices; 

COUNT()|
-------+
2382643|
```


#### exit from clickhouse-client and container
```commandline
9dfe07f25e30 :) exit;

root@9dfe07f25e30:/# exit
```
## Python virtual environment
```commandline
conda create -n chplenv python=3.11

conda activate chplenv
```

### requirements.txt
```commandline
clickhouse-sqlalchemy
jupyterlab
polars
pandas
sqlalchemy
pyarrow>=8.0.0
```

### Install requirements
```commandline
pip install -r requirements.txt
```

### Start jupyter lab
```commandline
jupyter lab --ip 0.0.0.0 --port 8888
```




