import logging

logger = dict()
logger["infra"] = logging.getLogger("infra")
logger["test"] = logging.getLogger("test")

system_event_logs = None

cluster_util = bucket_util = serverless_util = None

# Minimize paramiko module logs
logging.getLogger("paramiko").setLevel(logging.WARNING)
