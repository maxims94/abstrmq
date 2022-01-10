import logging
import logging.config
import json

from abstrmq.rmqnode import RMQNode
from src.server_app import ServerApp

log_config_path = f'config.json'
#log_config_path = f'config_prod.json'
with open(log_config_path) as f:
    logging.config.dictConfig(json.load(f))

log = logging.getLogger('main')

node = RMQNode(ServerApp, restart=True, max_conn_tries=3, conn_retry_interval=1)
node.start()
