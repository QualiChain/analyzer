from elasticsearch import Elasticsearch

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
        print(index)
        print(kwargs)
