from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import random
import time

import pypaques.logging

logger = pypaques.logging.get_logger(__name__)


class HttpError(Exception):
    pass


class Http503Error(HttpError):
    pass


class PaquesError(Exception):
    pass


class PaquesQueryError(Exception):
    def __init__(self, error):
        self._error = error

    @property
    def error_code(self):
        return self._error.get('errorCode', None)

    @property
    def error_name(self):
        return self._error.get('errorName', None)

    @property
    def error_type(self):
        return self._error.get('errorType', None)

    @property
    def error_exception(self):
        return self.failure_info.get('type', None) if self.failure_info else None

    @property
    def failure_info(self):
        return self._error.get('failureInfo', None)

    @property
    def message(self):
        return self._error.get(
            'message',
            'Paques did no return an error message',
        )

    @property
    def error_location(self):
        location = self._error['errorLocation']
        return (location['lineNumber'], location['columnNumber'])

    def __repr__(self):
        return '{}(type={}, name={}, message="{}")'.format(
            self.__class__.__name__,
            self.error_type,
            self.error_name,
            self.message,
        )

    def __str__(self):
        return repr(self)


class PaquesExternalError(PaquesQueryError):
    pass


class PaquesInternalError(PaquesQueryError):
    pass


class PaquesUserError(PaquesQueryError):
    pass


def retry_with(handle_retry, exceptions, conditions, max_attempts):
    def wrapper(func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            error = None
            result = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if any(guard(result) for guard in conditions):
                        handle_retry.retry(func, args, kwargs, None, attempt)
                        continue
                    return result
                except Exception as err:
                    error = err
                    if any(isinstance(err, exc) for exc in exceptions):
                        handle_retry.retry(func, args, kwargs, err, attempt)
                        continue
                    break
            logger.info('failed after {} attempts'.format(attempt))
            if error is not None:
                raise error
            return result
        return decorated
    return wrapper


class DelayExponential(object):
    def __init__(
        self,
        base=0.1,  # 100ms
        exponent=2,
        jitter=True,
        max_delay=2 * 3600,  # 2 hours
    ):
        self._base = base
        self._exponent = exponent
        self._jitter = jitter
        self._max_delay = max_delay

    def __call__(self, attempt):
        delay = float(self._base) * (self._exponent ** attempt)
        if self._jitter:
            delay *= random.random()
        delay = min(float(self._max_delay), delay)
        return delay


class RetryWithExponentialBackoff(object):
    def __init__(
        self,
        base=0.1,  # 100ms
        exponent=2,
        jitter=True,
        max_delay=2 * 3600  # 2 hours
    ):
        self._get_delay = DelayExponential(
            base, exponent, jitter, max_delay)

    def retry(self, func, args, kwargs, err, attempt):
        delay = self._get_delay(attempt)
        time.sleep(delay)