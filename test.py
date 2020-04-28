from sqlalchemy import create_engine

engine = create_engine('postgresql://admin:admin@qualichain.epu.ntua.gr:5432/api_db')
# connection = engine.connect()

print(engine.table_names())
connection.close()