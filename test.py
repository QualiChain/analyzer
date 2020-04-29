import pandas
import pandas as pd
from elasticsearch.helpers import bulk
from numpy import int64
from pymongo import MongoClient
from sqlalchemy import create_engine

from clients.elastic_client import ElasticClient
from clients.mongo_connector import MongoDBConnector
from utils import map_dtype_to_elk_type, df_lookup

# engine = create_engine("postgresql://admin:admin@qualichain.epu.ntua.gr:5432/api_db")
# table_df = pd.read_sql_table('job_post', engine)
#
# print(table_df.shape)
#
# new_table = table_df.drop_duplicates(subset="job_url")
# print(new_table.shape)
#
#
# # documents = table_df.to_dict(orient='records')
# # ELK = ElasticClient()
# # es = ELK.es_obj
# # bulk(es, documents, index='my_index', raise_on_error=True)
#
# # print(table_df.name)
# # print(df_lookup(table_df))
part = 'empDetails'

mongo_uri = 'mongodb://my_user:password123@localhost:27017/my_database'

m = MongoDBConnector(part)
res = m.check_if_uri_is_valid(uri=mongo_uri)
if res:

    data = pandas.DataFrame(list(m.collection.find()))
    print(data.head())