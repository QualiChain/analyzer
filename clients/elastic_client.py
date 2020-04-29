from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from settings import ELASTIC_HOSTNAME, ELASTIC_PORT


class ElasticClient(object):
    """This Python Object is used to handle ElasticSearch requests"""

    def __init__(self):
        self.es_obj = Elasticsearch([{
            'host': ELASTIC_HOSTNAME,
            'port': ELASTIC_PORT
        }])  # Initialize ElasticSearch Connection

    def create_index(self, index, **kwargs):
        """
        This function is used to create index using provided kwargs for index properties

        Args:
            index: index name
            **kwargs: provided index properties

        Returns: None

        """
        index_properties = kwargs

        index_body = {
            "mappings": index_properties
        }
        self.es_obj.indices.create(index=index, body=index_body, ignore=400)

    def insert_source_data(self, data_frame, index):
        """
        This function is used to take data frame's records and inserts them to provided index
        Args:
            data_frame: pandas data frame
            index: provided index

        Returns: None

        """
        df_documents = data_frame.to_dict(orient='records')
        bulk(self.es_obj, df_documents, index=index, raise_on_error=True)

