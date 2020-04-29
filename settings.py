import numpy as np

# APPLICATION SETTINGS

API_PORT = 5000

RDBMS_TYPES = ['MYSQL', 'POSTGRES', 'MARIADB']
SUPPORTED_TYPES = {
    np.int64: 'integer',
    np.object: 'text',
    np.float64: 'float',
    np.bool: 'boolean'
}
