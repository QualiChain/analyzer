# import pandas as pd
# from numpy import int64
# from sqlalchemy import create_engine
#
# from utils import map_dtype_to_elk_type, df_lookup
#
# engine = create_engine("postgresql://admin:admin@qualichain.epu.ntua.gr:5432/api_db")
# table_df = pd.read_sql_table('job_post', engine)
#
# print(table_df.name)
# print(df_lookup(table_df))
from clients.elastic_client import ElasticClient

es_obj = ElasticClient()

es_obj.create_index(index="some", a=1, b=2)