import pandas as pd

from clients.elastic_client import ElasticClient
from clients.rdbms_client import RDBMSClient
from utils import replace_nan_in_files, df_lookup


class DataPipeline(object):

    def __init__(self, index2use):
        self.es = ElasticClient()
        self.index = index2use

    @staticmethod
    def process_input(input_type, **kwargs):
        """
        The following function is used to process provided input per input type
        Args:
            input_type: input type provided by user
            **kwargs: provided kwargs

        Returns: data in data frame format

        """

        if input_type == 'RDBMS':
            input_uri = kwargs['input_uri']
            part = kwargs['part']

            rdbms = RDBMSClient(input_uri)
            provided_data = rdbms.load_table(part)

        elif input_type == 'TSV' or input_type == 'CSV':
            file_df = kwargs['data_frame']
            provided_data = replace_nan_in_files(file_df)

        elif input_type == 'MONGODB':
            provided_data = kwargs['data_frame']

        else:
            print('Provided input {} is not supported'.format(input_type), flush=True)
            provided_data = pd.DataFrame()
        return provided_data

    def append_to_elastic_search(self, data):
        """
        Data after loaded in pandas DataFrame format

        Args:
            data: pandas DataFrame

        Returns: None

        """
        input_types = df_lookup(data)
        self.es.create_index(index=self.index, properties=input_types)
        self.es.insert_source_data(data, self.index)

    def execute(self, input_type, **kwargs):
        """This function is used to execute the above functions"""
        data = self.process_input(input_type, **kwargs)

        if not data.empty:
            self.append_to_elastic_search(data=data)
            print('Data stored to ElasticSearch', flush=True)
        else:
            print("Empty Data", flush=True)
