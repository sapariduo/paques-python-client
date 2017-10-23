import requests
import ujson
import sseclient
import pprint
import pandas as pd

from pypaques import constants
from pypaques import exceptions
import pypaques.logging

MAX_ATTEMPTS = constants.DEFAULT_MAX_ATTEMPTS

logger = pypaques.logging.get_logger(__name__)

class ClientSession(object):
    def __init__(
        self,
        source,
        user,
        properties=None,
        headers=None,
    ):
        self.source = source
        self.user = user
        if properties is None:
            properties = {}
        self._properties = properties
        self._headers = headers or {}

    @property
    def properties(self):
        return self._properties

    @property
    def headers(self):
        return self._headers


def get_header_values(headers, header):
    return [val.strip() for val in headers[header].split(',')]


def get_session_property_values(headers, header):
    kvs = get_header_values(headers, header)
    return [
        (k.strip(), v.strip()) for k, v
        in (kv.split('=', 1) for kv in kvs)
        ]

class PaquesStatus(object):
    def __init__(self, id, node_url, event):
        self.id = id
        self.node_url = node_url
        self.event = event


    def __repr__(self):
        return (
            'PaquesStatus('
            'id={}, response_url={} , event={}'
            ')'.format(
                self.id,
                self.node_url,
                self.event,
            )
        )

class PaquesRequest(object):
    http = requests

    def __init__(
        self,
        host,  # type: Text
        port,  # type: int
        user,  # type: Text
        source=None,  # type: Text
        session_properties=None,  # type: Optional[Dict[Text, Any]]
        http_headers=None,  # type: Optional[Dict[Text, Text]]
        http_scheme=constants.HTTP,  # type: Text
        auth=constants.DEFAULT_AUTH,  # type: Optional[Any]
        max_attempts=MAX_ATTEMPTS,  # type: int
        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,  # type: Union[float, Tuple[float, float]]
        handle_retry=exceptions.RetryWithExponentialBackoff(),
        
    ):
        # type: (...) -> None
        self._client_session = ClientSession(
            source,
            user,
            session_properties,
            http_headers,
        )

        self._host = host
        self._port = port
        # mypy cannot follow module import
        self._event = None #tobe reviewed
        self._pipes = None #tobe reviewed
        self._all = None #tobe reviewed
        self._node_url = None #tobe reviewed
        self._http_session = self.http.Session()  # type: ignore
        self._http_session.headers.update(self.http_headers)
        self._auth = auth
        if self._auth:
            if http_scheme == constants.HTTP:
                raise ValueError('cannot use authentication with HTTP')
            self._auth.set_http_session(self._http_session)

        self._request_timeout = request_timeout
        self._handle_retry = handle_retry
        self.max_attempts = max_attempts
        self._http_scheme = http_scheme

    @property
    def http_headers(self):
        # type: () -> Dict[Text, Text]
        headers = {}

        headers[constants.HEADER_SOURCE] = self._client_session.source
        headers[constants.HEADER_USER] = self._client_session.user

        headers[constants.HEADER_SESSION] = ','.join(
            # ``name`` must not contain ``=``
            '{}={}'.format(name, value)
            for name, value in self._client_session.properties.items()
        )

        # merge custom http headers
        for key in self._client_session.headers:
            if key in headers.keys():
                raise ValueError('cannot override reserved HTTP header {}'.format(key))
        headers.update(self._client_session.headers)



        return headers

    @property
    def max_attempts(self):
        # type: () -> int
        return self._max_attempts

    @max_attempts.setter
    def max_attempts(self, value):
        # type: (int) -> None
        self._max_attempts = value
        if value == 1:  # No retry
            self._get = self._http_session.get
            self._post = self._http_session.post
            self._delete = self._http_session.delete
            return

        with_retry = exceptions.retry_with(
            self._handle_retry,
            exceptions=(
                PaquesRequest.http.ConnectionError,  # type: ignore
                PaquesRequest.http.Timeout,  # type: ignore
            ),
            conditions=(
                # need retry when there is no exception but the status code is 503
                lambda response: getattr(response, 'status_code', None) == 503,
            ),
            max_attempts=self._max_attempts,
        )
        self._get = with_retry(self._http_session.get)
        self._post = with_retry(self._http_session.post)

    def get_url(self, path):
        # type: (Text) -> Text
        return "{protocol}://{host}:{port}{path}".format(
            protocol=self._http_scheme,
            host=self._host,
            port=self._port,
            path=path
        )

    def get_node_url(self):
        # type: (Text) -> Text
        return "{protocol}://{host}:{port}".format(
            protocol=self._http_scheme,
            host=self._node_url,
            port=self._port
        )    
    
    @property
    def statement_url(self):
        # type: () -> Text
        return self.get_url(constants.URL_STATEMENT_PATH)

    @property
    def node_url(self):
        # type: () -> Text
        return self.get_node_url()
    
  
    @property
    def event(self):
        # type: () -> Text
        return self._event

    def post(self, pql):
#         data = pql.encode('utf-8')
        json = pql
        http_headers = self.http_headers

        http_response = self._post(
            self.statement_url,
            json=json,
            headers=http_headers,
            timeout=self._request_timeout
        )

        return http_response

    def get(self, node, params):
        self._node_url= node
        return self._get(
            self.node_url,
            params=params,
            stream=True,
        )

    def _process_error(self, error):
        error_type = error['errorType']
        if error_type == 'EXTERNAL':
            raise exceptions.PaquesExternalError(error)
        elif error_type == 'USER_ERROR':
            return exceptions.PaquesUserError(error)

        return exceptions.PaqeusQueryError(error)

    def raise_response_error(self, http_response):
        if http_response.status_code == 503:
            raise exceptions.Http503Error('error 503: service unavailable')

        raise exceptions.HttpError(
            'error {}{}'.format(
                http_response.status_code,
                ': {}'.format(http_response.content) if http_response.content else '',
            )
        )

    def process(self, http_response):
        # type: (requests.Response) -> PaquesStatus
        if not http_response.ok:
            self.raise_response_error(http_response)

        http_response.encoding = 'utf-8'
        response = http_response.json()
        logger.debug('HTTP {}: {}'.format(http_response.status_code, response))
        if 'error' in response:
            raise self._process_error(response['error'])

        if constants.HEADER_CLEAR_SESSION in http_response.headers:
            for prop in get_header_values(
                response.headers,
                constants.HEADER_CLEAR_SESSION,
            ):
                self._client_session.properties.pop(prop, None)

        if constants.HEADER_SET_SESSION in http_response.headers:
            for key, value in get_session_property_values(
                    response.headers,
                    constants.HEADER_SET_SESSION,
                ):
                self._client_session.properties[key] = value


        return PaquesStatus(
            id=response['data']['body']['quid'],
            node_url = response['data']['body']['explain']['nodes'][0]['publish_host'],
            event= response.get('event')
        )
    


class PaquesResult(object):
    """
    Represent the result of a Paques query as an iterator on rows.
    This class implements the iterator protocol as a generator type
    https://docs.python.org/3/library/stdtypes.html#generator-types
    """

    def __init__(self, data):
        self._results = data or []
#         self._rows = rows or []
        self._tables = {}
        self.datasetup()
        
#         self._rownumber = 0

    @property
    def tables(self):
        # type: Dict
        return self._tables.keys()
    
    @property
    def dataframe(self):
        # type:  List
        return self._dataframe
    
    
    def datasetup(self):
        try:
            for i in self._results:
                if i['event'] == 'data':
                    x = i['data']['rset']['source']
                    self._tables[x] = {'columns': [], 'rows': []}
            
            for i in self._results:
                try:
                    dataset = i['data']['rset']
                    if dataset['source'] in self._tables.keys():

                        for col in dataset['columns']:
                            if col not in self._tables[dataset['source']]['columns']:
                                self._tables[dataset['source']]['columns'].append(col)

                        self._tables[dataset['source']]['rows'].extend(dataset['rows'])

                except:
                    continue
            
        
        except:
            pass
        
        self._dataframe = {table: pd.DataFrame.from_records(self._tables[table]['rows'], columns=self._tables[table]['columns']) for table in self.tables}    


class PaquesQuery(object):
    """Represent the execution of a PQL statement by Paques."""
    def __init__(
        self,
        request,  # type: PaquesRequest
        pql=None,  # type: Text
    ):
        # type: (...) -> None
        self.query_id = None  # type: Optional[Text]
        self.node_url = None

#         self._stats = {}  # type: Dict[Any, Any]
        self._columns = None  # type: Optional[List[Text]]

        self._finished = False
        self._cancelled = False
        self._request = request
        self._pql = pql
        self._list = []
        # self._result = PaquesResult(self)

    @property
    def columns(self):
        return self._columns

    @property
    def stats(self):
        return self._stats
    
    @property
    def result(self):
        return self._result
    
    @property
    def list(self):
        return self._list
    
    def load(self):
        #type: () -> Paquesresult

        response = self._request.post(self._pql)
        status = self._request.process(response)
        self.query_id = status.id
        self.node_url = status.node_url
        self._stats = {u'queryId': self.query_id}
        return status
    
    def execute(self, node, id):
        # type: () -> PaquesResult
        """
        HTTP reques sent to master coordinator, It set query id and 
        returns  a stream data as query result.  To fetch result object, 
        call fetch() and result will be sent to datastore

        """
        if self._cancelled:
            raise exceptions.PaquesUserError("Query has been cancelled")
         
        params = {u'event': 'stream', u'quid': id}
        response = self._request.get(self.node_url, params)
        _response = sseclient.SSEClient(response)
        for event in _response.events():
            self._list.append(ujson.loads(event.data))
                
        self._result = PaquesResult(self.list)
        return self._result

    def fetch(self):
        # type: () -> List[List[Any]]
        """Continue fetching data for the current query_id"""
        response = self._request.get(self._request.next_uri)
        status = self._request.process(response)
        if status.columns:
            self._columns = status.columns
        self._stats.update(status.stats)
        logger.debug(status)
        if status.next_uri is None:
            self._finished = True
        return status.rows

    def cancel(self):
        # type: () -> None
        """Cancel the current query"""
        if self.is_finished():
            return

        self._cancelled = True
        if self._request.next_uri is None:
            return

        response = self._request.delete(self._request.next_uri)
        if response.status_code == requests.codes.no_content:
            return
        self._request.raise_response_error(response)

    def is_finished(self):
        # type: () -> bool
        return self._finished