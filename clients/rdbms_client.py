import pandas as pd
from sqlalchemy import create_engine


class RDBMSClient(object):
    """This Python Object is used to receive data from Relational Databases"""

    def __init__(self, connection_uri):
        self.engine = create_engine(connection_uri)

    def load_table(self, table):
        """
        This function is used to load provided table from Relational Database

        Args:
            table: provided table name

        Returns: table in pandas data frame format

        """
        table_df = pd.read_sql_table(table, self.engine)
        return table_df
