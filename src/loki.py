# A fixed version of https://github.com/hack-han/loki-client
from urllib.parse import urlparse, urlencode
import requests
from requests import JSONDecodeError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

# Support Loki version:2.4.2
MAX_REQUEST_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1
RETRY_ON_STATUS = [408, 429, 500, 502, 503, 504]
SUPPORTED_DIRECTION = ["backward", "forward"]
# the duration before end time when get context
CONTEXT_HOURS_DELTA = 1
DEFAULT_HOURS_DELTA = 2 * 24


class LokiClient:
    """
    Loki client for Python to communicate with Loki server.
    Ref: https://grafana.com/docs/loki/v2.4/api/
    """

    def __init__(self,
                 url: str = "http://127.0.0.1:3100",
                 headers: dict = None,
                 disable_ssl: bool = True,
                 retry: Retry = None,
                 hours_delta=DEFAULT_HOURS_DELTA):
        """
        constructor
        :param url:
        :param headers:
        :param disable_ssl:
        :param retry:
        :param hours_delta:
        :return:
        """
        if url is None:
            raise TypeError("Url can not be empty!")

        self.headers = headers
        self.url = url
        self.loki_host = urlparse(self.url).netloc
        self._all_metrics = None
        self.ssl_verification = not disable_ssl
        # the days between start and end time
        self.hours_delta = hours_delta
        # the time range when searching context for one key line
        self.context_timedelta = int(CONTEXT_HOURS_DELTA * 3600 * 10 ** 9)

        if retry is None:
            retry = Retry(total=MAX_REQUEST_RETRIES, backoff_factor=RETRY_BACKOFF_FACTOR,
                          status_forcelist=RETRY_ON_STATUS)

        self.__session = requests.Session()
        self.__session.mount(self.url, HTTPAdapter(max_retries=retry))
        self.__session.keep_alive = False

    def ready(self) -> bool:
        """
        Check whether Loki host is ready to accept traffic.
        Ref: https://grafana.com/docs/loki/v2.4/api/#get-ready
        :return:
        bool: True if Loki is ready, False otherwise.
        """
        try:
            response = self.__session.get(
                url="{}/ready".format(self.url),
                verify=self.ssl_verification,
                headers=self.headers
            )
            return response.ok
        except Exception:
            return False

    def query_range(self,
                    query: str,
                    limit: int = 100,
                    start: datetime = None,
                    end: datetime = None,
                    direction: str = SUPPORTED_DIRECTION[0],
                    params: dict = None) -> dict:
        """
        Query logs from Loki, corresponding query_range.
        Ref: GET /loki/api/v1/query_range
        :param query:
        :param limit:
        :param start:
        :param end:
        :param direction:
        :param params:
        :return:
        """
        params = params or {}
        if query:
            if not isinstance(query, str):
                raise AttributeError('Incorrect query type {}, should be type {}.'.format(type(query), str))
            params['query'] = query
        else:
            raise AttributeError('Param query can not be empty.')

        if end:
            if not isinstance(end, datetime):
                raise AttributeError('Incorrect end type {}, should be type {}.'.format(type(end), datetime))
            # Convert to int, or will be scientific notation, which will result in request exception
            params['end'] = int(end.timestamp() * 10 ** 9)
        else:
            params['end'] = int(datetime.now().timestamp() * 10 ** 9)

        if start:
            if not isinstance(start, datetime):
                raise AttributeError('Incorrect start type {}, should be type {}.'.format(type(start), datetime))
            # Convert to int, or will be scientific notation, which will result in request exception
            params['start'] = int(start.timestamp() * 10 ** 9)
        else:
            params['start'] = int((datetime.fromtimestamp(params['end'] / 10 ** 9) - timedelta(
                hours=self.hours_delta)).timestamp() * 10 ** 9)

        if limit:
            params['limit'] = limit
        else:
            raise AttributeError('The value of limit is not correct.')

        if direction not in SUPPORTED_DIRECTION:
            return False, {'message': 'Invalid direction value: {}.'.format(direction)}
        params['direction'] = direction

        enc_query = urlencode(params)
        target_url = '{}/loki/api/v1/query_range?{}'.format(self.url, enc_query)

        try:
            response = self.__session.get(
                url=target_url,
                verify=self.ssl_verification,
                headers=self.headers
            )
            return response.json()
        except JSONDecodeError as e:
            return e.doc
        except Exception as ex:
            # return False, {'message': repr(ex)}
            raise ex
