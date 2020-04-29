import pandas as pd
from elasticsearch.helpers import bulk
from numpy import int64
from sqlalchemy import create_engine

from clients.elastic_client import ElasticClient
from utils import map_dtype_to_elk_type, df_lookup

engine = create_engine("postgresql://admin:admin@qualichain.epu.ntua.gr:5432/api_db")
table_df = pd.read_sql_table('job_post', engine)

print(table_df.shape)

new_table = table_df.drop_duplicates(subset="job_url")
print(new_table.shape)


# documents = table_df.to_dict(orient='records')
# ELK = ElasticClient()
# es = ELK.es_obj
# bulk(es, documents, index='my_index', raise_on_error=True)

# print(table_df.name)
# print(df_lookup(table_df))