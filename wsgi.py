import logging
import sys

from app.app import app

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

log = logging.getLogger(__name__)

if __name__ == '__main__':
    log.info("Starting AnalEyeZer")
    app.run(host='0.0.0.0', port=API_PORT, debug=True)
