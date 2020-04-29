import logging
import sys

import pandas as pd
from flask import Flask, request
from flask_restful import Api, Resource

from clients.mongo_connector import MongoDBConnector
from settings import RDBMS_TYPES
from utils import check_input_data, test_pipeline, rdbms_check_if_uri_is_valid, test_rdbms_pipeline, test_mongo_pipeline

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
api = Api(app)


class ReceiveDataSource(Resource):
    """
    This Class is used to handle incoming resources to AnalEyeZer
    """

    def post(self):
        """Receive new Resource"""

        data = request.get_json()

        data_format = check_input_data(data)
        if data_format:
            log.info('Data Formatted correctly')

            TYPE = data['type']
            index2use = data['index']
            part = data['part']
            uri = data['uri']

            if TYPE in RDBMS_TYPES:

                _check = rdbms_check_if_uri_is_valid(
                    input_uri=uri,
                    part=part
                )
                if _check:
                    test_rdbms_pipeline(
                        input_uri=uri,
                        part=part,
                        index2use=index2use
                    )
                    response_msg = {'message': 'Data Source send for processing'}
                    return response_msg, 201
                else:
                    response_msg = {'message': 'Invalid Data Source'}
                    return response_msg, 400

            elif TYPE == 'MONGODB':

                mongo_client = MongoDBConnector(part)
                _check = mongo_client.check_if_uri_is_valid(uri)

                if _check:
                    mongodb_data = mongo_client.load_collection()
                    test_mongo_pipeline(mongodb_data, index2use)
                    response_msg = {'message': 'Data Source send for processing'}
                    return response_msg, 201
                else:
                    response_msg = {'message': 'Invalid Data Source'}
                    return response_msg, 400

            else:
                response_msg = {'message': 'Invalid Data Source'}
                return response_msg, 400
        else:
            log.error('Data malformed')
            response_msg = {'message': 'Your input must contain uri, data source type and part'}
            return response_msg, 400


class ReceiveFiles(Resource):
    """
    This Python Object is used to receive the provided CSV file and load it to ElasticSearch
    Compatible formats are CSV and TSV files
    """

    def post(self):
        """This function is used to receive Files Data"""
        try:
            file = request.files['file']
            index2use = request.form['index']
            file_type = request.form['type']

            if file_type == 'csv':

                file_df = pd.read_csv(file)
                test_pipeline(file_df, index2use)

                response_msg = {'message': 'CSV File received for processing'}
                return response_msg, 201

            elif file_type == 'tsv':

                file_df = pd.read_csv(file, sep='\t')
                test_pipeline(file_df, index2use)

                response_msg = {'message': 'TSV File received for processing'}
                return response_msg, 201

            else:
                response_msg = {'message': 'Unsupported file type'}
                return response_msg, 400
        except Exception as ex:
            log.error(ex)
            return ex, 400


# Routes
api.add_resource(ReceiveDataSource, '/receive/source')
api.add_resource(ReceiveFiles, '/upload/file')
