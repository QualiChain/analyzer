import logging
import sys

import pandas as pd
from flask import Flask, request
from flask_restful import Api, Resource

from utils import check_input_data, check_source

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
                return response_msg, 201
            else:
                response_msg = {'message': 'Invalid Data Source'}
                return response_msg, 400
        else:
            log.error('Data malformed')
            response_msg = {'message': 'Your input must contain uri, data source type and part'}
            return response_msg, 400


class ReceiveCSVFiles(Resource):

    def post(self):
        file = request.files['file']
        data = pd.read_csv(file, header=None)
        print(data.iloc[0].loc[1])


# Routes
api.add_resource(ReceiveDataSource, '/receive/source')
api.add_resource(ReceiveCSVFiles, '/upload/csv')
