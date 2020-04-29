import pandas as pd
from numpy import int64
from sqlalchemy import create_engine

from settings import SUPPORTED_TYPES
from utils import map_dtype_to_elk_type

engine = create_engine("postgresql://admin:admin@qualichain.epu.ntua.gr:5432/api_db")
table_df = pd.read_sql_table('job_post', engine)

# type_items = table_df.dtypes.items()
# for item in type_items:
#     print(map_dtype_to_elk_type(item[1]))

print(dict(map(lambda x: (x[0], map_dtype_to_elk_type(x[1])), table_df.dtypes.items())))