from waitress import serve
import flask_app
import logging

logger = logging.getLogger('waitress')
logger.setLevel(logging.INFO)


serve(flask_app.app, host='0.0.0.0', port=8080, ident=None)