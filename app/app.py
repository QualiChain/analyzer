import logging
import sys

import numpy
import pandas as pd
from flask import Flask, request
from flask_restful import Api, Resource

from clients.elastic_client import ElasticClient
from clients.rdbms_client import RDBMSClient
from utils import check_input_data, check_source, df_lookup, replace_nan_in_files

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

            _check = check_source(
                input_uri=data['uri'],
                type=data['type'],
                part=data['part']
            )
            if _check:
                response_msg = {'message': 'Data Source send for processing'}

                index2use = data['index']

                ## for testing purposes , later in async way

                rdbms = RDBMSClient(data['uri'])
                table_df = rdbms.load_table(data['part'])
                input_types = df_lookup(table_df)

                es = ElasticClient()
                es.create_index(index=index2use, properties=input_types)

                es.insert_source_data(table_df, index2use)
                ##

                return response_msg, 201
            else:
                response_msg = {'message': 'Invalid Data Source'}
                return response_msg, 400
        else:
            log.error('Data malformed')
            response_msg = {'message': 'Your input must contain uri, data source type and part'}
            return response_msg, 400


class ReceiveCSVFiles(Resource):
    """
    This Python Object is used to receive the provided CSV file and load it to ElasticSearch
    """

    def post(self):
        try:
            file = request.files['file']
            index2use = request.form['index']

            csv_df = pd.read_csv(file)

            ## for testing purposes , later in async way
            csv_without_nan = replace_nan_in_files(csv_df)
            input_types = df_lookup(csv_without_nan)

            es = ElasticClient()
            es.create_index(index=index2use, properties=input_types)
            es.insert_source_data(csv_without_nan, index2use)
            ##

            response_msg = {'message': 'CSV File received for processing'}
            return response_msg, 201
        except Exception as ex:
            log.error(ex)
            return ex, 400


# Routes
api.add_resource(ReceiveDataSource, '/receive/source')
api.add_resource(ReceiveCSVFiles, '/upload/csv')
