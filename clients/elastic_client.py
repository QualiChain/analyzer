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

    def get_aggregations(self, results):
        """This function is used when aggregation functionalities are requested"""
        aggregations = results['aggregations']
        return aggregations

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

    def choose_query(self, params):
        """
        This function is used to choose which elasticsearch function will be executed

        Args:
            params: provided request parameters

        Returns: elastic search results

        """
        query_type = params['query']

        if query_type == 'bool_query':

            results = self.bool_queries(**params)

            if "aggs" in params.keys():
                response = self.get_aggregations(results), 201
            else:
                response = results['hits']['hits'], 201

        elif query_type == 'match_documents':

            results = self.match_documents(**params)

            if "aggs" in params.keys():
                response = self.get_aggregations(results), 201
            else:
                response = results['hits']['hits'], 201

        elif query_type == 'list_documents':

            results = self.list_documents(**params)
            response = results['hits']['hits'], 201

        elif query_type == 'get_document':
            results = self.get_document(**params)
            response = results, 201

        elif query_type == 'mget_documents':
            results = self.mget_docs(**params)
            response = {'docs': [_doc['_source'] for _doc in results['docs']]}, 201

        elif query_type == 'cat_indices':
            results = self.cat_indices()
            response = {'stored_indices': results}, 201

        elif query_type == 'get_index':
            results = self.get_index(**params)
            return results
        elif query_type == 'create_index':
            new_index = params["index"]

            del params['query']
            del params['index']

            index_properties = {
                'properties': params
            }
            self.create_index(index=new_index, **index_properties)
            response = {'msg': "Index: {} created".format(new_index)}, 201
        elif query_type == 'create_document':
            index = params['index']

            del params['query']
            del params['index']

            self.create_document(index=index, **params)
            response = {'msg': "Document created"}, 201

        elif query_type == 'delete_document':

            index = params['index']
            self.delete_document(index=index, id=params['id'])
            response = {'msg': "Document: {} removed".format(params['id'])}, 201
        elif query_type == 'script_update':

            index = params['index']
            id = params['id']
            body = params['body']

            self.script_update(index=index, id=id, upscript=body)
            response = {'msg': "Document: {} updated".format(params['id'])}, 201

        else:
            response = {'message': 'Query: ({}) not supported'.format(query_type)}, 400

        return response

    def cat_indices(self, **kwargs):
        """
        This function is used to retrieve all stored indices

        Returns: stored indices

        """
        indices = list(
            self.es_obj.indices.get_alias("*")
        )
        return indices

    def get_index(self, index, **kwargs):
        """
        This function is used to return info and mappings about provided index

        Args:
            index: provided index
            **kwargs: provided kwargs

        Returns: index info

        """
        index_info = self.es_obj.indices.get(index=index)
        return index_info

    def create_document(self, index, **kwargs):
        """This function is used to create a document to a specific index"""
        body = kwargs
        if 'id' in body.keys():
            doc_id = body['id']
        else:
            doc_id = None
        self.es_obj.index(index=index, body=body, id=doc_id)

    def bool_queries(self, index, min_score=3, _source=[], size=HITS_SIZE, **kwargs):
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

        if 'aggs' in kwargs.keys():
            body['aggs'] = kwargs['aggs']

        results = self.es_obj.search(index=index, body=body, size=size)
        return results

    def match_documents(self, index, _source=[], size=HITS_SIZE, **kwargs):
        """
        This function is used to perform match queries against stored documents that belong to
        provided Index

        Args:
            index: provided index
            _source: provided source
            **kwargs: provided kwargs

        Returns: documents that match search term

        """
        term = kwargs["term"]
        fields = kwargs["fields"]

        body = {
            "query": {
                "multi_match": {
                    "query": term,
                    "fields": fields
                }
            }
        }

        if 'aggs' in kwargs.keys():
            body['aggs'] = kwargs['aggs']

        results = self.es_obj.search(index=index, body=body, size=size)
        return results

    def list_documents(self, index, _source=[], **kwargs):
        """
        The following function is used to retrieve stored documents on specific index

        Args:
            index: provided index
            _source: document fields to return

        Returns: stored documents

        """
        body = {
            "query": {
                "match_all": {}
            }
        }
        results = self.es_obj.search(index=index, body=body, size=HITS_SIZE)
        return results

    def get_document(self, index, id, _source=[], **kwargs):
        """
        This function is used to retrieve specific information for provided index

        Args:
            index: provided index
            id: document id
            _source: document fields to return
            **kwargs: provided kwargs

        Returns: specific document

        """
        document = self.es_obj.get_source(index=index, id=str(id))
        return document

    def delete_document(self, index, id):
        """This function is used to delete a document"""
        self.es_obj.delete(index=index, id=str(id))

    def mget_docs(self, index, ids, _source=[], **kwargs):
        """
        This function is used to retrieve multiple documents using their IDs

        Args:
            index: provided Index
            ids: provided IDs
            _source: document fields to return
            **kwargs: provided kwargs

        Returns: elk documents

        """
        results = self.es_obj.mget(
            index=index,
            body={'ids': ids}
        )
        return results

    def script_update(self, index, id, upscript):
        """This function is used to update a document using scripts"""
        self.es_obj.update(index=index, id=id, body=upscript)
