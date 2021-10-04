import logging

logger = dict()
logger["infra"] = logging.getLogger("infra")
logger["test"] = logging.getLogger("test")

system_event_logs = None
