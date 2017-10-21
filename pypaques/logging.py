from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

LEVEL = logging.INFO


# TODO: provide interface to use ``logging.dictConfig``
def get_logger(name, log_level=LEVEL):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    return logger