from sqlalchemy import create_engine

engine = create_engine("mariadb://root@localhost:3306")
# connection = engine.connect()

print(engine.table_names())
connection.close()