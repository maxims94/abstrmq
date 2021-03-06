import logging
import logging.config
import json

from abstrmq.rmqnode import RMQNode
from src.app import stBaseApp

log_config_path = f'config.json'
with open(log_config_path) as f:
    logging.config.dictConfig(json.load(f))

log = logging.getLogger('main')

node = RMQNode(stBaseApp, restart=True, max_conn_tries=3, conn_retry_interval=1)
node.start()
