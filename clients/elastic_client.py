from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from settings import ELASTIC_HOSTNAME, ELASTIC_PORT, HITS_SIZE


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

    def bool_queries(self, index, min_score=3, _source=[], **kwargs):
        """
        This function is used to execute boolean queries in elasticsearch

        Args:
            index: index to search for documents
            min_score: min score
            _source: returned fields
            **kwargs: provided kwargs

        Returns: elasticsearch results

        """

        should = kwargs['should'] if 'should' in kwargs.keys() else []
        must = kwargs['must'] if 'must' in kwargs.keys() else []
        must_not = kwargs['must_not'] if 'must_not' in kwargs.keys() else []

        body = {
            "_source": _source,
            "min_score": min_score,
            "query": {
                "bool": {
                    "should": should,
                    "must": must,
                    "must_not": must_not
                }
            }
        }
        results = self.es_obj.search(index=index, body=body, size=HITS_SIZE)
        return results
