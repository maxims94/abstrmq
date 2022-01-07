import logging
import logging.config
import json

from abstrmq.rmqnode import RMQNode
from src.test_future_queue_app import TestFutureQueueApp
from src.test_managed_queue_app import TestManagedQueueApp

log_config_path = f'config.json'
with open(log_config_path) as f:
    logging.config.dictConfig(json.load(f))

log = logging.getLogger('main')

node = RMQNode(TestManagedQueueApp, restart=True, max_conn_tries=3, conn_retry_interval=1)
#node = RMQNode(TestFutureQueueApp, restart=True, max_conn_tries=3, conn_retry_interval=1)
node.start()
