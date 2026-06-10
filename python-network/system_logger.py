import logging
import logging.handlers
import queue
import sys
from typing import Optional, Tuple

Listener = Optional[logging.handlers.QueueListener]


def setup_logger(
    name: str,
    log_path: Optional[str] = None,
    *,
    disable: bool = False,
    level: int = logging.INFO,
) -> Tuple[logging.Logger, Listener]:

    logger = logging.getLogger(name)
    logger.handlers = []
    logger.propagate = False

    if disable:
        logger.addHandler(logging.NullHandler())
        logger.setLevel(level)
        return logger, None

    log_queue = queue.Queue(-1)

    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if log_path:
        handler = logging.FileHandler(log_path, mode="w")
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setFormatter(formatter)

    listener = logging.handlers.QueueListener(
        log_queue, handler, respect_handler_level=True
    )
    listener.start()

    logger.addHandler(logging.handlers.QueueHandler(log_queue))
    logger.setLevel(level)

    return logger, listener


def stop_logger(listener: Listener) -> None:
    if listener is not None:
        listener.stop()
