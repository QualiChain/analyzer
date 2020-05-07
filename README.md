## AnalEyeZer

AnalEyeZer is a component used in Qualichain H2020

### Supported Data Sources
+ RDBMS
    + PostgreSQL
+ NoSQL
    + MongoDB
+ Files
  + CSV
  + TSV


### Setup ElasticSearch and Kibana

+ `cd config`
+ `docker-compose up -d es01 kibana`

### Run AnalEyeZer locally


+ `pip install -r requirements.txt`
+ `flask run`

### Examples

**Submit a new data source**

```http request
POST /receive/source HTTP/1.1
Host: 127.0.0.1:5000
Content-Type: application/json

{
	"uri": "postgresql://admin:admin@qualichain.epu.ntua.gr:5432/api_db",
	"type": "POSTGRES",
	"part": "job_post",
	"index": "my_index"
}
```

**Submit Query to ElasticSearch**
```http request
POST /ask/storage HTTP/1.1
Host: 127.0.0.1:5000
Content-Type: application/json

{
	"query":"bool_query",
	"index": "my_index",
	"min_score": 4,
	"_source": ["id"],
	"should": [
    {"multi_match": {
        "query": "backend engineer",
        "fields": ["title", "requirements"],
        "type": "phrase",
        "slop": 2}
    },
    {"multi_match": {
        "query": "backend developer",
        "fields": ["title", "requirements"],
        "type": "phrase",
        "slop": 2}
    }
]
}
```