import os

# APPLICATION SETTINGS
API_PORT = os.environ.get('API_PORT', 5000)
RDBMS_TYPES = ['MYSQL', 'POSTGRES', 'MARIADB']

# ELASTICSEARCH SETTINGS
ELASTIC_HOSTNAME = os.environ.get('ELASTIC_HOSTNAME', 'qualichain.epu.ntua.gr')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', 9200)
HITS_SIZE = os.environ.get('HITS_SIZE', 10000)
