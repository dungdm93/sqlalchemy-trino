import logging
import os

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] [%(levelname)-5s] %(name)-15s:%(lineno)d: %(message)s')

SUPERSET_WEBSERVER_PROTOCOL = 'http'
SUPERSET_WEBSERVER_ADDRESS = '0.0.0.0'
SUPERSET_WEBSERVER_PORT = 8088
ENABLE_PROXY_FIX = True
SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:SuperSecr3t@postgres:5432/postgres'

if "SUPERSET_HOME" in os.environ:
    DATA_DIR = os.environ["SUPERSET_HOME"]
else:
    DATA_DIR = os.path.join(os.path.expanduser("~"), ".superset")

UPLOAD_FOLDER = DATA_DIR + "/app/static/uploads/"
IMG_UPLOAD_FOLDER = DATA_DIR + "/app/static/uploads/"
