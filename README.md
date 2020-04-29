## AnalEyeZer

AnalEyeZer is a component used in Qualichain H2020

### Supported Data Sources
+ RDBMS
    + PostgreSQL
+ Files
  + CSV


### Setup ElasticSearch and Kibana

+ `cd config`
+ `docker-compose up -d es01 kibana`

### Run AnalEyeZer locally


+ `pip insta -r requirements.txt`
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