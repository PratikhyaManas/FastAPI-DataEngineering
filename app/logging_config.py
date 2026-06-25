import logging
import sys

logger = logging.getLogger("pipeline-api")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s",
    "%Y-%m-%dT%H:%M:%S",
)
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)
