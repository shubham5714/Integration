"""DRX Elasticsearch integration for non-Demisto execution.

``main(integration_id, command)`` loads config and state from Supabase,
runs the command, and persists ``last_run``. I/O uses embedded ``RuntimeContext``
and helpers (formerly ``runtime.py`` / ``common_utils.py``) in this single file.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import traceback
import warnings
from dataclasses import dataclass
import dataclasses as _dc
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urljoin

import dateparser
import requests
import urllib3
from dateutil.parser import parse
from prefect import flow, task
from prefect.blocks.system import Secret

try:
    from supabase import Client as SupabaseClient, create_client  # type: ignore[import-not-found]

    SUPABASE_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency at runtime
    SupabaseClient = Any  # type: ignore[assignment,misc]
    create_client = None  # type: ignore[assignment]
    SUPABASE_AVAILABLE = False


# ---------------------------------------------------------------------------
# Embedded: runtime (was ``elastic/runtime.py``)
# ---------------------------------------------------------------------------


class IntegrationError(Exception):
    """Raised when the integration encounters a fatal error."""

    # Replaces Demisto ``return_error`` with an exception callers can catch.


class Logger:
    """Thin logger wrapper that mirrors ``demisto.debug/info/error`` semantics."""

    def __init__(self, name: str = "drx-elasticsearch", level: int = logging.INFO) -> None:
        import sys as _sys

        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            handler = logging.StreamHandler(_sys.stdout)
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
            )
            self._logger.addHandler(handler)
        self._logger.setLevel(level)
        self._logger.propagate = False

    def debug(self, msg: Any) -> None:
        self._logger.debug(str(msg))

    def info(self, msg: Any) -> None:
        self._logger.info(str(msg))

    def error(self, msg: Any) -> None:
        self._logger.error(str(msg))

    def __call__(self, msg: Any) -> None:
        self.info(msg)


@dataclass
class StatePort:
    """In-memory state holder for last_run and integration context."""

    last_run: Dict[str, Any] = _dc.field(default_factory=dict)
    integration_context: Dict[str, Any] = _dc.field(default_factory=dict)

    def get_last_run(self) -> Dict[str, Any]:
        return dict(self.last_run)

    def set_last_run(self, data: Optional[Dict[str, Any]]) -> None:
        self.last_run = dict(data) if data else {}

    def get_integration_context(self) -> Dict[str, Any]:
        return dict(self.integration_context)

    def set_integration_context(self, data: Optional[Dict[str, Any]]) -> None:
        self.integration_context = dict(data) if data else {}


@dataclass
class OutputPort:
    """Captures emitted results so the caller can inspect them after run."""

    results: List[Any] = _dc.field(default_factory=list)
    incidents: List[Dict[str, Any]] = _dc.field(default_factory=list)
    errors: List[str] = _dc.field(default_factory=list)

    def emit_results(self, value: Any) -> None:
        self.results.append(value)

    def emit_incidents(self, incidents: List[Dict[str, Any]]) -> None:
        if incidents:
            self.incidents.extend(incidents)

    def emit_error(self, message: str, raise_after: bool = True) -> None:
        self.errors.append(message)
        if raise_after:
            raise IntegrationError(message)


@dataclass
class RuntimeContext:
    """Holds all execution-time inputs and ports for the integration."""

    params: Dict[str, Any] = _dc.field(default_factory=dict)
    args: Dict[str, Any] = _dc.field(default_factory=dict)
    command: str = ""
    logger: Logger = _dc.field(default_factory=Logger)
    state: StatePort = _dc.field(default_factory=StatePort)
    output: OutputPort = _dc.field(default_factory=OutputPort)

    @classmethod
    def from_payload(cls, payload: Optional[Dict[str, Any]]) -> "RuntimeContext":
        payload = dict(payload or {})
        state_data = payload.get("state") or {}

        log_level_name = (payload.get("log_level") or "INFO").upper()
        log_level = getattr(logging, log_level_name, logging.INFO)

        return cls(
            params=dict(payload.get("params") or {}),
            args=dict(payload.get("args") or {}),
            command=str(payload.get("command") or ""),
            logger=Logger(level=log_level),
            state=StatePort(
                last_run=dict(state_data.get("last_run") or {}),
                integration_context=dict(state_data.get("integration_context") or {}),
            ),
            output=OutputPort(),
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "results": list(self.output.results),
            "incidents": list(self.output.incidents),
            "errors": list(self.output.errors),
            "state": {
                "last_run": self.state.get_last_run(),
                "integration_context": self.state.get_integration_context(),
            },
        }


# ---------------------------------------------------------------------------
# Embedded: common_utils (was ``elastic/common_utils.py``)
# ---------------------------------------------------------------------------


class DemistoException(IntegrationError):
    """Backwards-compatible alias for code that still raises ``DemistoException``."""


@dataclass
class CommandResults:
    """Plain-Python equivalent of the Demisto ``CommandResults`` model."""

    outputs_prefix: Optional[str] = None
    outputs_key_field: Optional[str] = None
    outputs: Any = None
    raw_response: Any = None
    readable_output: Optional[str] = None
    indicator: Any = None
    indicators: Any = None
    tags: Optional[List[str]] = _dc.field(default=None)
    extra: dict = _dc.field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "outputs_prefix": self.outputs_prefix,
            "outputs_key_field": self.outputs_key_field,
            "outputs": self.outputs,
            "raw_response": self.raw_response,
            "readable_output": self.readable_output,
        }


def argToBoolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("y", "yes", "true", "1", "t"):
            return True
        if normalized in ("n", "no", "false", "0", "f", ""):
            return False
    raise ValueError(f"Cannot convert {value!r} to boolean")


def arg_to_number(
    arg: Any, arg_name: Optional[str] = None, required: bool = False
) -> Optional[int]:
    if arg is None or arg == "":
        if required:
            raise ValueError(f"Missing required argument: {arg_name or 'arg'}")
        return None
    if isinstance(arg, bool):
        raise ValueError(f"Argument '{arg_name}' is boolean, expected number")
    if isinstance(arg, int):
        return arg
    if isinstance(arg, float):
        return int(arg)
    if isinstance(arg, str):
        try:
            return int(arg)
        except ValueError:
            try:
                return int(float(arg))
            except ValueError as exc:
                raise ValueError(
                    f"Argument '{arg_name}' is not a number: {arg!r}"
                ) from exc
    raise ValueError(f"Argument '{arg_name}' has unsupported type: {type(arg)}")


def handle_proxy(
    proxy_param_name: str = "proxy",
    checkbox_default_value: bool = False,
    params: Optional[dict] = None,
) -> dict:
    params = params or {}
    use_proxy = params.get(proxy_param_name, checkbox_default_value)
    if not use_proxy:
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            os.environ.pop(key, None)
        return {}

    proxies: dict = {}
    http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    return proxies


def _stringify_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return text.replace("\n", " ").replace("|", "\\|")


def tableToMarkdown(
    name: str,
    t: Any,
    headers: Optional[Iterable[str]] = None,
    headerTransform=None,
    removeNull: bool = False,
    metadata: Any = None,
    url_keys: Any = None,
    **_kwargs: Any,
) -> str:
    title = f"### {name}\n"
    if t is None:
        return f"{title}**No entries.**\n"

    rows: List[dict]
    if isinstance(t, dict):
        rows = [t]
    elif isinstance(t, list):
        rows = [r for r in t if isinstance(r, dict)]
    else:
        return f"{title}{t}\n"

    if not rows:
        return f"{title}**No entries.**\n"

    if headers is None:
        seen: List[str] = []
        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.append(key)
        headers_list = seen
    else:
        headers_list = list(headers)

    if removeNull:
        filtered = []
        for header in headers_list:
            for row in rows:
                value = row.get(header)
                if value not in (None, "", [], {}):
                    filtered.append(header)
                    break
        headers_list = filtered

    if not headers_list:
        return f"{title}**No entries.**\n"

    display_headers = (
        [headerTransform(h) for h in headers_list]
        if headerTransform is not None
        else list(headers_list)
    )

    lines = [f"### {name}"]
    lines.append("|" + "|".join(_stringify_cell(h) for h in display_headers) + "|")
    lines.append("|" + "|".join(["---"] * len(display_headers)) + "|")
    for row in rows:
        cells = [_stringify_cell(row.get(h, "")) for h in headers_list]
        lines.append("|" + "|".join(cells) + "|")

    return "\n".join(lines) + "\n"


urllib3.disable_warnings()
warnings.filterwarnings(
    action="ignore",
    message=".*using SSL with verify_certs=False is insecure.",
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASIC_AUTH = "Basic auth"
BEARER_AUTH = "Bearer auth"
API_KEY_AUTH = "API key auth"
API_KEY_PREFIX = "_api_key_id:"

ELASTICSEARCH_V8 = "Elasticsearch_v8"
ELASTICSEARCH_V9 = "Elasticsearch_v9"
OPEN_SEARCH = "OpenSearch"

ES_DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"
PYTHON_DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
SUPABASE_URL = "https://zhhsijigoupqroztdrdy.supabase.co"

supabase_api_key = await Secret.load("supabase-api-key")
SUPABASE_ANON_KEY = supabase_api_key.get()

# Same incident table as ``drx-securonix.insert_incident_row_in_supabase``.
SUPABASE_DEV_TICKETS_TABLE = "dev_tickets"
# Fields kept in-memory for fetch/export but not persisted on insert.
SUPABASE_INCIDENT_INSERT_OMIT_KEYS = frozenset({"event_count", "rule_type", "query"})

HTTP_ERRORS = {
    400: "400 Bad Request - Incorrect or invalid parameters",
    401: "401 Unauthorized - Incorrect or invalid username or password",
    403: "403 Forbidden - The account does not support performing this task",
    404: "404 Not Found - Elasticsearch server was not found",
    408: "408 Timeout - Check port number or Elasticsearch server credentials",
    410: "410 Gone - Elasticsearch server no longer exists in the service",
    500: "500 Internal Server Error - Internal error",
    503: "503 Service Unavailable",
}


# ---------------------------------------------------------------------------
# Module-level state populated by ``init`` from the runtime params
# ---------------------------------------------------------------------------

_runtime: Optional[RuntimeContext] = None

PARAMS: Dict[str, Any] = {}
AUTH_TYPE: str = BASIC_AUTH
USERNAME: Optional[str] = None
PASSWORD: Optional[str] = None
API_KEY_ID: Optional[str] = None
API_KEY_SECRET: Optional[str] = None
API_KEY: Optional[tuple] = None
ELASTIC_SEARCH_CLIENT: str = ""
SERVER: str = ""
PROXY: Any = None
TIME_FIELD: str = ""
FETCH_INDEX: str = ""
FETCH_QUERY_PARM: str = ""
RAW_QUERY: str = ""
FETCH_TIME: str = "3 days"
FETCH_SIZE: int = 50
RAW_LOGS_FETCH_SIZE: int = 2
INSECURE: bool = True
TIME_METHOD: str = "Simple-Date"
TIMEOUT: int = 60
MAP_LABELS: bool = True
FETCH_QUERY: str = ""

# Elasticsearch / OpenSearch library handles set by ``_import_es_libraries``.
Search: Any = None
QueryString: Any = None
Elasticsearch: Any = None
NotFoundError: Any = None
RequestsHttpConnection: Any = None
RequestsHttpNode: Any = None


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


def _runtime_or_raise() -> RuntimeContext:
    if _runtime is None:
        raise IntegrationError(
            "Runtime not initialized. Invoke ``main(payload)`` before using "
            "module-level helpers."
        )
    return _runtime


def _log():
    return _runtime_or_raise().logger


def _state():
    return _runtime_or_raise().state


def _output():
    return _runtime_or_raise().output


def _import_es_libraries(client_type: str) -> None:
    """Imports the right Elasticsearch/OpenSearch SDK based on ``client_type``.

    Library handles are stored as module-level globals so the rest of the
    integration code can reference them as before.
    """
    global Search, QueryString, Elasticsearch, NotFoundError
    global RequestsHttpConnection, RequestsHttpNode

    if client_type == OPEN_SEARCH:
        from opensearch_dsl import Search as _Search
        from opensearch_dsl.query import QueryString as _QueryString
        from opensearchpy import (
            NotFoundError as _NotFoundError,
            OpenSearch as _Elasticsearch,
            RequestsHttpConnection as _RequestsHttpConnection,
        )

        Search = _Search
        QueryString = _QueryString
        Elasticsearch = _Elasticsearch
        NotFoundError = _NotFoundError
        RequestsHttpConnection = _RequestsHttpConnection
        RequestsHttpNode = None

    elif client_type in (ELASTICSEARCH_V8, ELASTICSEARCH_V9):
        from elastic_transport import RequestsHttpNode as _RequestsHttpNode
        from elasticsearch import (  # type: ignore[assignment]
            Elasticsearch as _Elasticsearch,
            NotFoundError as _NotFoundError,
        )
        from elasticsearch.dsl import Search as _Search
        from elasticsearch.dsl.query import QueryString as _QueryString

        Search = _Search
        QueryString = _QueryString
        Elasticsearch = _Elasticsearch
        NotFoundError = _NotFoundError
        RequestsHttpNode = _RequestsHttpNode
        RequestsHttpConnection = None

    else:  # Elasticsearch (<= v7)
        from elasticsearch7 import (  # type: ignore[assignment,misc]
            Elasticsearch as _Elasticsearch,
            NotFoundError as _NotFoundError,
            RequestsHttpConnection as _RequestsHttpConnection,
        )
        from elasticsearch.dsl import Search as _Search
        from elasticsearch.dsl.query import QueryString as _QueryString

        Search = _Search
        QueryString = _QueryString
        Elasticsearch = _Elasticsearch
        NotFoundError = _NotFoundError
        RequestsHttpConnection = _RequestsHttpConnection
        RequestsHttpNode = None


def _init_globals_from_params(params: Dict[str, Any]) -> None:
    """Populate the module-level configuration globals from a params dict."""
    global PARAMS, AUTH_TYPE, USERNAME, PASSWORD, API_KEY_ID, API_KEY_SECRET, API_KEY
    global ELASTIC_SEARCH_CLIENT, SERVER, PROXY
    global TIME_FIELD, FETCH_INDEX, FETCH_QUERY_PARM, RAW_QUERY, FETCH_TIME, FETCH_SIZE
    global RAW_LOGS_FETCH_SIZE
    global INSECURE, TIME_METHOD, TIMEOUT, MAP_LABELS, FETCH_QUERY

    PARAMS = dict(params or {})

    AUTH_TYPE = PARAMS.get("auth_type", BASIC_AUTH)
    credentials = PARAMS.get("credentials") or {}
    USERNAME = credentials.get("identifier")
    PASSWORD = credentials.get("password")
    api_key_credentials = PARAMS.get("api_key_auth_credentials") or {}
    API_KEY_ID = api_key_credentials.get("identifier")
    API_KEY_SECRET = api_key_credentials.get("password")
    API_KEY = None

    if AUTH_TYPE == BASIC_AUTH and USERNAME and USERNAME.startswith(API_KEY_PREFIX):
        AUTH_TYPE = API_KEY_AUTH
        API_KEY_ID = USERNAME[len(API_KEY_PREFIX):]
        API_KEY = (API_KEY_ID, PASSWORD)
    elif AUTH_TYPE == API_KEY_AUTH:
        API_KEY = (API_KEY_ID, API_KEY_SECRET)

    ELASTIC_SEARCH_CLIENT = PARAMS.get("client_type", "")
    SERVER = (PARAMS.get("url") or "").rstrip("/")
    PROXY = PARAMS.get("proxy")

    TIME_FIELD = PARAMS.get("fetch_time_field", "")
    FETCH_INDEX = PARAMS.get("fetch_index", "")
    FETCH_QUERY_PARM = PARAMS.get("fetch_query", "")
    RAW_QUERY = PARAMS.get("raw_query", "")
    FETCH_TIME = PARAMS.get("fetch_time", "3 days")
    FETCH_SIZE = int(PARAMS.get("fetch_size", 50))
    _raw_logs_n = int(PARAMS.get("raw_logs_fetch_size", 2))
    RAW_LOGS_FETCH_SIZE = max(1, min(_raw_logs_n, 500))
    INSECURE = not PARAMS.get("insecure", False)
    TIME_METHOD = PARAMS.get("time_method", "Simple-Date")
    TIMEOUT = int(PARAMS.get("timeout") or 60)
    MAP_LABELS = PARAMS.get("map_labels", True)
    FETCH_QUERY = RAW_QUERY or FETCH_QUERY_PARM


def init(runtime: RuntimeContext, *, import_libraries: bool = True) -> None:
    """Bind a runtime context to this module and seed configuration globals."""
    global _runtime
    _runtime = runtime
    _init_globals_from_params(runtime.params)
    if import_libraries:
        _import_es_libraries(ELASTIC_SEARCH_CLIENT)


# ---------------------------------------------------------------------------
# Helper utilities (no platform dependencies)
# ---------------------------------------------------------------------------


def get_value_by_dot_notation(dictionary, key):
    """Get a nested value from ``dictionary`` using ``key`` in dot notation."""
    value = dictionary
    _log().debug("Trying to get value by dot notation")
    for k in key.split("."):
        if isinstance(value, dict):
            value = value.get(k)
        else:
            _log().debug(f"Last value is not a dict, returning None. {value=}")
            return None
    return value


def convert_date_to_timestamp(date):
    """Convert ``date`` into the format expected by the configured time method."""
    _log().debug(f"Converting date to timestamp: {date}")
    if str(date).isdigit():
        return int(date)

    if TIME_METHOD == "Timestamp-Seconds":
        return int(date.timestamp())

    if TIME_METHOD == "Timestamp-Milliseconds":
        return int(date.timestamp() * 1000)

    return datetime.strftime(date, PYTHON_DEFAULT_DATETIME_FORMAT)


def timestamp_to_date(timestamp_string):
    """Convert a timestamp string to a ``datetime`` object."""
    timestamp_number: float
    if TIME_METHOD == "Timestamp-Milliseconds":
        timestamp_number = float(int(timestamp_string) / 1000)
    else:
        _log().debug(f"{TIME_METHOD=}. Should be Timestamp-Seconds.")
        timestamp_number = float(timestamp_string)

    return datetime.utcfromtimestamp(timestamp_number)


def get_api_key_header_val(api_key):
    """Return the ``ApiKey ...`` header value for the given API key."""
    if isinstance(api_key, (tuple, list)):
        s = f"{api_key[0]}:{api_key[1]}".encode()
        return "ApiKey " + base64.b64encode(s).decode("utf-8")
    return "ApiKey " + api_key


def is_access_token_expired(expires_in: str) -> bool:
    """Return ``True`` if the token expires within one minute (or is invalid)."""
    try:
        expiration_time = datetime.strptime(
            expires_in, "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=None)

        current_time_with_buffer = datetime.utcnow() + timedelta(minutes=1)

        if expiration_time > current_time_with_buffer:
            _log().debug(
                f"is_access_token_expired - using existing Access token "
                f"from integration context (expires in {expires_in})."
            )
            return False
        _log().debug("is_access_token_expired - Access token expired.")
        return True
    except (ValueError, TypeError) as e:
        _log().debug(
            f"is_access_token_expired - Error parsing expiration time: {e}. "
            "Treating as expired."
        )
        return True


def get_elastic_token():
    """Authenticate via OAuth 2.0 and return a valid access token."""
    try:
        url = urljoin(SERVER, "_security/oauth2/token")
        headers = {"Content-Type": "application/json"}

        integration_context = _state().get_integration_context()
        access_token = integration_context.get("access_token", "")
        access_token_expires_in = integration_context.get("access_token_expires_in", "")
        refresh_token = integration_context.get("refresh_token", "")
        refresh_token_expires_in = integration_context.get("refresh_token_expires_in", "")

        if access_token and not is_access_token_expired(access_token_expires_in):
            _log().debug(
                "get_elastic_token - Using existing access token from integration context."
            )
            return access_token

        if not USERNAME or not PASSWORD:
            _log().debug("get_elastic_token - username or password fields are missing.")
            raise DemistoException("username or password fields are missing.")

        if refresh_token and not is_access_token_expired(refresh_token_expires_in):
            _log().debug(
                "get_elastic_token - Access token expired, but Refresh token valid. "
                "Attempting to get token using refresh token"
            )

            payload = {"grant_type": "refresh_token", "refresh_token": refresh_token}
            response = requests.post(
                url, headers=headers, json=payload, verify=INSECURE,
                auth=(USERNAME, PASSWORD),
            )

            if response.status_code == 200:
                now = datetime.utcnow()
                token_data = response.json()
                access_token_expires_in = (
                    now + timedelta(seconds=token_data.get("expires_in"))
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
                refresh_token_expires_in = (
                    now + timedelta(hours=24)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")

                integration_context.update(
                    {
                        "access_token": token_data.get("access_token"),
                        "refresh_token": token_data.get("refresh_token"),
                        "access_token_expires_in": access_token_expires_in,
                        "refresh_token_expires_in": refresh_token_expires_in,
                    }
                )
                _state().set_integration_context(integration_context)
                _log().debug(
                    "get_elastic_token - Access token received successfully by refresh "
                    "token and set to integration context."
                )
                return integration_context["access_token"]

            _log().debug(
                "get_elastic_token - refresh fails, a new token will be generated "
                "via password grant."
            )
            integration_context.update(
                {"refresh_token": None, "refresh_token_expires_in": None}
            )
            _state().set_integration_context(integration_context)

        _log().debug("get_elastic_token - Attempting to get token using grant_type:password")

        payload = {"grant_type": "password", "username": USERNAME, "password": PASSWORD}
        response = requests.post(
            url, headers=headers, auth=(USERNAME, PASSWORD), json=payload, verify=INSECURE,
        )
        if response.status_code == 200:
            now = datetime.utcnow()
            token_data = response.json()
            access_token_expires_in = (
                now + timedelta(seconds=token_data.get("expires_in"))
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            refresh_token_expires_in = (
                now + timedelta(hours=24)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")

            integration_context.update(
                {
                    "access_token": token_data.get("access_token"),
                    "refresh_token": token_data.get("refresh_token"),
                    "access_token_expires_in": access_token_expires_in,
                    "refresh_token_expires_in": refresh_token_expires_in,
                }
            )
            _state().set_integration_context(integration_context)
            _log().debug(
                "get_elastic_token - Access token received successfully via password "
                "grant and set to integration context."
            )
            return integration_context["access_token"]

        _log().debug(f"Failed to authenticate: {response.status_code}\n{response.text}")
        try:
            reason = json.loads(response.text).get("error", {}).get("reason")
        except Exception:
            reason = response.reason or response.text
        raise DemistoException(f"{response.status_code}, {reason}")

    except Exception as e:
        _log().debug(f"get_elastic_token error: \n{str(e)}")
        raise DemistoException(f"{str(e)}")


def elasticsearch_builder(proxies):
    """Build an Elasticsearch client honouring auth, proxies and TLS settings."""
    connection_args: Dict[str, Any] = {
        "hosts": [SERVER],
        "verify_certs": INSECURE,
        "timeout": TIMEOUT,
    }
    _log().debug(f"Building Elasticsearch client with args: {connection_args}")

    if ELASTIC_SEARCH_CLIENT not in (ELASTICSEARCH_V9, ELASTICSEARCH_V8):
        connection_args["connection_class"] = RequestsHttpConnection
        connection_args["proxies"] = proxies
    else:
        # Elastic v8/v9 client uses elastic-transport with custom node class for proxy.
        class CustomHttpNode(RequestsHttpNode):  # type: ignore[misc,valid-type]
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.session.proxies = proxies

        connection_args["node_class"] = CustomHttpNode

    if AUTH_TYPE == API_KEY_AUTH and API_KEY:
        connection_args["api_key"] = API_KEY
    elif AUTH_TYPE == BASIC_AUTH and USERNAME and PASSWORD:
        if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8):
            connection_args["basic_auth"] = (USERNAME, PASSWORD)
        else:
            connection_args["http_auth"] = (USERNAME, PASSWORD)
    elif AUTH_TYPE == BEARER_AUTH:
        connection_args["bearer_auth"] = get_elastic_token()

    es = Elasticsearch(**connection_args)

    if (
        AUTH_TYPE == API_KEY_AUTH
        and hasattr(es, "transport")
        and hasattr(es.transport, "get_connection")
    ):
        es.transport.get_connection().session.headers["authorization"] = (
            get_api_key_header_val(API_KEY)
        )

    return es


def get_hit_table(hit):
    """Build a context dict and header list for a single search hit."""
    table_context = {
        "_index": hit.get("_index"),
        "_id": hit.get("_id"),
        "_type": hit.get("_type"),
        "_score": hit.get("_score"),
    }
    headers = ["_index", "_id", "_type", "_score"]
    if hit.get("_source") is not None:
        for source_field in hit.get("_source"):
            table_context[str(source_field)] = hit.get("_source").get(str(source_field))
            headers.append(source_field)
    return table_context, headers


def results_to_context(index, query, base_page, size, total_dict, response, event=False):
    """Build the full context payload for a search response."""
    search_context = {
        "Server": SERVER,
        "Index": index,
        "Query": query,
        "Page": base_page,
        "Size": size,
        "total": total_dict,
        "max_score": response.get("hits").get("max_score"),
        "took": response.get("took"),
        "timed_out": response.get("timed_out"),
    }

    if aggregations := response.get("aggregations"):
        search_context["aggregations"] = aggregations

    hit_headers: List = []
    hit_tables = []
    if total_dict.get("value") > 0:
        if not event:
            results = response.get("hits").get("hits", [])
        else:
            results = response.get("hits").get("events", [])

        for hit in results:
            single_hit_table, single_header = get_hit_table(hit)
            hit_tables.append(single_hit_table)
            hit_headers = list(
                set(single_header + hit_headers) - {"_id", "_type", "_index", "_score"}
            )
        hit_headers = ["_id", "_index", "_type", "_score"] + hit_headers

    search_context["Results"] = response.get("hits").get("hits")
    meta_headers = [
        "Query", "took", "timed_out", "total", "max_score",
        "Server", "Page", "Size", "aggregations",
    ]
    return search_context, meta_headers, hit_tables, hit_headers


def get_total_results(response_dict):
    """Return ``(total_dict, total_count)`` derived from a search response."""
    total_results = response_dict.get("hits", {}).get("total")
    if not str(total_results).isdigit():
        # Elasticsearch v7+ returns ``{"value": N, "relation": ...}``.
        total_results = total_results.get("value")
        total_dict = response_dict.get("hits").get("total")
    else:
        total_dict = {"value": total_results}
    return total_dict, total_results


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def search_command(args, proxies):
    """Execute the generic search command."""
    index = args.get("index")
    query = args.get("query")
    fields = args.get("fields")
    explain = (args.get("explain", "false") or "false").lower() == "true"
    base_page = int(args.get("page", 0))
    size = int(args.get("size", 0))
    sort_field = args.get("sort-field")
    sort_order = args.get("sort-order")
    query_dsl = args.get("query_dsl")
    timestamp_field = args.get("timestamp_field")
    timestamp_range_start = args.get("timestamp_range_start")
    timestamp_range_end = args.get("timestamp_range_end")

    if query and query_dsl:
        _output().emit_error(
            "Both query and query_dsl are configured. "
            "Please choose between query or query_dsl."
        )

    es = elasticsearch_builder(proxies)
    time_range_dict = None
    if timestamp_range_end or timestamp_range_start:
        time_range_dict = get_time_range(
            time_range_start=timestamp_range_start,
            time_range_end=timestamp_range_end,
            time_field=timestamp_field,
        )
    _log().debug(
        f"Executing search with index={index}, query={query}, query_dsl={query_dsl}"
    )

    if query_dsl:
        query_dsl = query_string_to_dict(query_dsl)
        if query_dsl.get("size", False) or query_dsl.get("page", False):
            response = execute_raw_query(es, query_dsl, index)
        else:
            response = execute_raw_query(es, query_dsl, index, size, base_page)
    else:
        que = QueryString(query=_normalize_query_string_operators(query))
        search = Search(using=es, index=index).query(que)[base_page: base_page + size]
        if explain:
            search = search.extra(explain=True)
        if time_range_dict:
            search = search.filter(time_range_dict)
        if fields is not None:
            fields = fields.split(",")
            search = search.source(fields)
        if sort_field is not None:
            search = search.sort({sort_field: {"order": sort_order}})

        if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8, OPEN_SEARCH):
            response = search.execute().to_dict()
        else:
            response = es.search(index=search._index, body=search.to_dict(), **search._params)

    _log().debug(f"Search response: {response}")
    total_dict, total_results = get_total_results(response)
    search_context, meta_headers, hit_tables, hit_headers = results_to_context(
        index, query_dsl or query, base_page, size, total_dict, response,
    )
    search_human_readable = tableToMarkdown(
        "Search Metadata:", search_context, meta_headers, removeNull=True
    )
    hits_human_readable = tableToMarkdown(
        "Hits:", hit_tables, hit_headers, removeNull=True
    )
    total_human_readable = search_human_readable + "\n" + hits_human_readable

    _output().emit_results(
        CommandResults(
            outputs_prefix="Elasticsearch.Search",
            outputs=search_context,
            raw_response=response,
            readable_output=total_human_readable,
        )
    )


def fetch_params_check():
    """Validate that all required fetch parameters have been configured."""
    str_error: List[str] = []
    if (TIME_FIELD == "" or TIME_FIELD is None) and not RAW_QUERY:
        str_error.append("Index time field is not configured.")

    if not FETCH_QUERY:
        str_error.append("Query by which to fetch incidents is not configured.")

    if RAW_QUERY and FETCH_QUERY_PARM:
        str_error.append(
            "Both Query and Raw Query are configured. "
            "Please choose between Query or Raw Query."
        )

    if str_error:
        _output().emit_error(
            "Got the following errors in test:\nFetches incidents is enabled.\n"
            + "\n".join(str_error)
        )


def test_query_to_fetch_incident_index(es):
    """Run a baseline query against ``FETCH_INDEX`` to confirm reachability."""
    try:
        query = QueryString(query="*")
        search = Search(using=es, index=FETCH_INDEX).query(query)[0:1]

        if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8):
            response = search.execute().to_dict()
        else:
            response = es.search(index=search._index, body=search.to_dict(), **search._params)

        _log().debug(f"Test query to fetch incident index response: {response}")
        get_total_results(response)
    except NotFoundError as e:
        _output().emit_error(
            "Fetch incidents test failed.\nError message: {}.".format(
                str(e).split(",")[2][2:-1]
            )
        )


def test_general_query(es):
    """Run a wildcard query across all indexes."""
    try:
        query = QueryString(query="*")
        search = Search(using=es, index="*").query(query)[0:1]

        if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8, OPEN_SEARCH):
            response = search.execute().to_dict()
        else:
            response = es.search(index=search._index, body=search.to_dict(), **search._params)

        _log().debug(f"Test general query response: {response}")
        get_total_results(response)
    except NotFoundError as e:
        _output().emit_error(
            f"Failed executing general search command - please check the Server URL and "
            f"port number and the supplied credentials.\nError message: {e!s}."
        )


def test_time_field_query(es):
    """Validate that ``TIME_FIELD`` exists in the configured ``FETCH_INDEX``."""
    query = QueryString(query=TIME_FIELD + ":*")
    search = Search(using=es, index=FETCH_INDEX).query(query)[0:1]

    if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8, OPEN_SEARCH):
        response = search.execute().to_dict()
    else:
        response = es.search(index=search._index, body=search.to_dict(), **search._params)

    _log().debug(f"Test time field query response: {response}")
    _, total_results = get_total_results(response)

    if total_results == 0:
        raise Exception(
            f"Fetch incidents test failed.\nDate field value incorrect [{TIME_FIELD}]."
        )
    return response


def test_fetch_query(es):
    """Run the configured fetch query and return the raw response."""
    query = QueryString(
        query=str(TIME_FIELD) + ":* AND " + _normalize_query_string_operators(FETCH_QUERY)
    )
    search = Search(using=es, index=FETCH_INDEX).query(query)[0:1]

    if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8, OPEN_SEARCH):
        response = search.execute().to_dict()
    else:
        response = es.search(index=search._index, body=search.to_dict(), **search._params)

    _log().debug(f"Test fetch query response: {response}")
    return response


def test_timestamp_format(timestamp):
    """Validate that ``timestamp`` matches the configured ``TIME_METHOD``."""
    timestamp_in_seconds_len = len(str(int(time.time())))

    if TIME_METHOD == "Timestamp-Seconds":
        if not timestamp.isdigit():
            _output().emit_error(
                f"The time field does not contain a standard timestamp.\nFetched: {timestamp}"
            )
        elif len(timestamp) > timestamp_in_seconds_len:
            _output().emit_error(
                f"Fetched timestamp is not in seconds since epoch.\nFetched: {timestamp}"
            )
    elif TIME_METHOD == "Timestamp-Milliseconds":
        if not timestamp.isdigit():
            _output().emit_error(
                f"The timestamp fetched is not in milliseconds.\nFetched: {timestamp}"
            )
        elif len(timestamp) <= timestamp_in_seconds_len:
            _output().emit_error(
                f"Fetched timestamp is not in milliseconds since epoch.\nFetched: {timestamp}"
            )


def test_connectivity_auth(proxies) -> tuple:
    """Probe ``SERVER`` to verify authentication settings work."""
    _log().debug("test_connectivity_auth started")
    headers = {"Content-Type": "application/json"}
    res = None

    try:
        if AUTH_TYPE == BASIC_AUTH:
            _log().debug(
                "test_connectivity_auth - Basic auth setting authorization header "
                "and sending request"
            )
            res = requests.get(
                SERVER, auth=(USERNAME, PASSWORD), verify=INSECURE, headers=headers
            )
        elif AUTH_TYPE == API_KEY_AUTH:
            _log().debug(
                "test_connectivity_auth - API key auth setting authorization header "
                "and sending request"
            )
            headers["authorization"] = get_api_key_header_val(API_KEY)
            res = requests.get(SERVER, verify=INSECURE, headers=headers)
        elif AUTH_TYPE == BEARER_AUTH:
            _log().debug(
                "test_connectivity_auth - Bearer auth setting authorization header "
                "and sending request"
            )
            headers["Authorization"] = f"Bearer {get_elastic_token()}"
            res = requests.get(SERVER, verify=INSECURE, headers=headers)

        if res is not None:
            if res.status_code >= 400:
                _log().debug(
                    f"test_connectivity_auth - Failed to connect.\n"
                    f"{res.status_code=}, {res.text=}"
                )
                return False, f"Failed to connect.\nStatus:{res.status_code}, {res.reason}"
            if res.status_code == 200:
                _log().debug("test_connectivity_auth - Connectivity test successful")
                verify_es_server_version(res.json())
                return True, "Connectivity test successful"
        return False, "No response received from server"
    except Exception as e:
        _log().debug(f"test_connectivity_auth - Failed to connect.\nError message: {e}")
        return False, f"Failed to connect.\n{e}"


def verify_es_server_version(res):
    """Validate that the configured ``client_type`` matches the server version."""
    es_server_version = res.get("version", {}).get("number", "")
    _log().debug(f"Elasticsearch server version is: {es_server_version}")
    if not es_server_version:
        return
    major_version = es_server_version.split(".")[0]
    if not major_version:
        return
    if int(major_version) >= 8 and ELASTIC_SEARCH_CLIENT not in (
        ELASTICSEARCH_V9, ELASTICSEARCH_V8, OPEN_SEARCH
    ):
        raise ValueError(
            f"Configuration Error: Your Elasticsearch server is version "
            f"{es_server_version}. Please ensure that the client type is set to "
            f"{ELASTICSEARCH_V9}, {ELASTICSEARCH_V8} or {OPEN_SEARCH}. "
            f"For more information please see the integration documentation."
        )
    if int(major_version) <= 7 and ELASTIC_SEARCH_CLIENT not in (
        OPEN_SEARCH, "Elasticsearch"
    ):
        raise ValueError(
            f"Configuration Error: Your Elasticsearch server is version "
            f"{es_server_version}. Please ensure that the client type is set to "
            f"Elasticsearch or {OPEN_SEARCH}. "
            f"For more information please see the integration documentation."
        )


@task(log_prints=True)
def test_func(proxies):
    """Implements ``test-module`` connectivity check."""
    success, message = test_connectivity_auth(proxies)
    if not success:
        return message
    if PARAMS.get("isFetch"):
        fetch_params_check()
    return "ok"


def integration_health_check(proxies):
    """Run a full diagnostic of fetch configuration and connectivity."""
    success, message = test_connectivity_auth(proxies)
    if not success:
        raise DemistoException(message)
    es = elasticsearch_builder(proxies)

    if PARAMS.get("isFetch"):
        fetch_params_check()
        hit_date = ""
        try:
            test_query_to_fetch_incident_index(es)
            response = test_time_field_query(es)
            source = response.get("hits", {}).get("hits")[0].get("_source", {})
            hit_date = str(get_value_by_dot_notation(source, str(TIME_FIELD)))
            _log().debug(f"Hit date received: {hit_date}")
            if "Timestamp" not in TIME_METHOD:
                parse(str(hit_date))
            else:
                test_timestamp_format(hit_date)
                timestamp_to_date(hit_date)
        except ValueError as e:
            _output().emit_error(
                "Inserted time format is incorrect.\n"
                + str(e) + "\n" + TIME_FIELD + " fetched: " + hit_date
            )

        try:
            if RAW_QUERY:
                fetch_result = execute_raw_query(es, RAW_QUERY)
            else:
                fetch_result = test_fetch_query(es)

            if fetch_result and isinstance(fetch_result.get("timed_out"), bool):
                if fetch_result.get("timed_out"):
                    _output().emit_error(
                        f"Elasticsearch fetching has timed out. Fetching response "
                        f"was:\n{str(fetch_result)}"
                    )
                _, total_results = get_total_results(fetch_result)
                if total_results == 0:
                    _log().info(
                        "Elasticsearch fetching test returned 0 hits, "
                        "but this might be expected."
                    )
            else:
                _output().emit_error(
                    "Elasticsearch fetching was unsuccessful. Fetching returned the "
                    "following invalid object:\n" + str(fetch_result)
                )
        except IntegrationError:
            raise
        except Exception as ex:
            _output().emit_error(
                f"An exception has been thrown trying to test Elasticsearch "
                f"fetching:\n{str(ex)}"
            )
    else:
        test_general_query(es)
    return "Testing was successful."


def incident_label_maker(source):
    """Create a Demisto-style ``labels`` list from a hit's ``_source`` mapping."""
    labels = []
    for field, value in source.items():
        encoded_value = value if isinstance(value, str) else json.dumps(value)
        labels.append({"type": str(field), "value": encoded_value})
    return labels


_INCIDENT_EXPORT_DEFAULTS: Dict[str, Any] = {
    "instance_name": "Embark-Elastic",
    "tenant_id": "d1708ffc-397e-43b6-8f0a-49306dcfc35d",
    "tenant_name": "Embark Group",
    "classifier": "test",
    "mapper": "test",
    "type": "elastic",
    "alert_source": "/assets/images/brand-logos/elastic-logo.png",
}


def _finalize_incident_for_export(incident: Dict[str, Any]) -> None:
    """Attach ``ai_message`` and default row fields before emit."""
    logs = incident.get("raw_logs")
    first = logs[0] if isinstance(logs, list) and logs else ""
    incident["ai_message"] = json.dumps(
        {
            "name": incident.get("name"),
            "severity": incident.get("severity"),
            "occurred_at": incident.get("occurred_at"),
            "raw_log": first,
        },
        default=str,
    )
    for k, v in _INCIDENT_EXPORT_DEFAULTS.items():
        incident.setdefault(k, v)


def results_to_incidents_timestamp(response, last_fetch, es):
    """Convert search hits into incidents using a numeric ``last_fetch`` cursor."""
    current_fetch = last_fetch
    incidents = []
    for hit in response.get("hits", {}).get("hits"):
        source = hit.get("_source")
        if source is None:
            continue
        time_field_value = get_value_by_dot_notation(source, str(TIME_FIELD))
        if time_field_value is None:
            continue

        hit_date = timestamp_to_date(str(time_field_value))
        hit_timestamp = int(time_field_value)

        if hit_timestamp > last_fetch:
            last_fetch = hit_timestamp

        if hit_timestamp > current_fetch:
            alert_name = source.get("kibana.alert.rule.name")
            alert_severity = source.get("kibana.alert.severity")
            alert_rule_uuid = source.get("kibana.alert.rule.uuid")
            inc = {
                "name": alert_name,
                "occurred_at": format_to_iso(hit_date.isoformat()),
                "severity": alert_severity,
                "rawJSON": json.dumps(hit),
                "source_id": alert_rule_uuid,
                "raw_logs": [],
                "event_count": _get_alert_field(source, "kibana.alert.threshold_result.count"),
                "rule_type": _get_alert_field(source, "kibana.alert.rule.type")
                or _get_alert_field(source, "kibana.alert.rule.parameters.type"),
            }
            if _is_threshold_rule_source(source):
                _enrich_incident_with_threshold_raw_logs(inc, es, source)
            elif _is_esql_rule_source(source):
                _enrich_incident_with_esql_raw_logs(inc, es, source)
            elif _is_query_rule_source(source):
                _enrich_incident_with_query_rule_raw_logs(inc, es, source)
            elif _is_new_terms_rule_source(source):
                _enrich_incident_with_new_terms_raw_logs(inc, es, source)
            elif _is_eql_rule_source(source):
                _enrich_incident_with_eql_rule_raw_logs(inc, es, source)
            elif _is_threat_match_rule_source(source):
                _enrich_incident_with_threat_match_raw_logs(inc, es, source)
            _finalize_incident_for_export(inc)
            incidents.append(inc)

    return incidents, last_fetch


def results_to_incidents_datetime(response, last_fetch, es):
    """Convert search hits into incidents using a datetime ``last_fetch`` cursor."""
    last_fetch = dateparser.parse(last_fetch)
    last_fetch_timestamp = int(last_fetch.timestamp() * 1000)
    current_fetch = last_fetch_timestamp
    incidents = []

    for hit in response.get("hits", {}).get("hits"):
        source = hit.get("_source")
        if source is None:
            continue
        time_field_value = get_value_by_dot_notation(source, str(TIME_FIELD))
        if time_field_value is None:
            continue

        hit_date = parse(str(time_field_value))
        hit_timestamp = int(hit_date.timestamp() * 1000)

        if hit_timestamp > last_fetch_timestamp:
            last_fetch = hit_date
            last_fetch_timestamp = hit_timestamp

        if hit_timestamp > current_fetch:
            alert_name = source.get("kibana.alert.rule.name")
            alert_severity = source.get("kibana.alert.severity")
            alert_rule_uuid = source.get("kibana.alert.rule.uuid")
            inc = {
                "name": alert_name,
                "occurred_at": format_to_iso(hit_date.isoformat()),
                "severity": alert_severity,
                "rawJSON": json.dumps(hit),
                "source_id": alert_rule_uuid,
                "raw_logs": [],
                "event_count": _get_alert_field(source, "kibana.alert.threshold_result.count"),
                "rule_type": _get_alert_field(source, "kibana.alert.rule.type")
                or _get_alert_field(source, "kibana.alert.rule.parameters.type"),
            }
            if _is_threshold_rule_source(source):
                _enrich_incident_with_threshold_raw_logs(inc, es, source)
            elif _is_esql_rule_source(source):
                _enrich_incident_with_esql_raw_logs(inc, es, source)
            elif _is_query_rule_source(source):
                _enrich_incident_with_query_rule_raw_logs(inc, es, source)
            elif _is_new_terms_rule_source(source):
                _enrich_incident_with_new_terms_raw_logs(inc, es, source)
            elif _is_eql_rule_source(source):
                _enrich_incident_with_eql_rule_raw_logs(inc, es, source)
            elif _is_threat_match_rule_source(source):
                _enrich_incident_with_threat_match_raw_logs(inc, es, source)
            _finalize_incident_for_export(inc)
            incidents.append(inc)
        else:
            _log().debug(
                f"Skipping hit ID: {hit.get('_id')} since {hit_timestamp=} "
                f"is earlier than the {current_fetch=}"
            )

    return incidents, last_fetch.isoformat()


def _extract_threshold_query_filters(rule_filters: Any) -> List[Dict[str, Any]]:
    """Convert Kibana rule filters into Elasticsearch bool filter clauses."""
    filters: List[Dict[str, Any]] = []
    if not isinstance(rule_filters, list):
        return filters

    for rule_filter in rule_filters:
        if not isinstance(rule_filter, dict):
            continue
        if (rule_filter.get("meta") or {}).get("disabled") is True:
            continue
        if isinstance(rule_filter.get("query"), dict):
            filters.append(rule_filter["query"])
            continue
        if any(key in rule_filter for key in ("term", "terms", "range", "exists", "bool", "match")):
            filters.append(rule_filter)
    return filters


def _get_alert_field(source: Dict[str, Any], key: str) -> Any:
    """Read alert field from flat dotted keys, mixed-prefix maps, or nested dicts."""
    if not isinstance(source, dict):
        return None
    if key in source:
        return source.get(key)

    # Support mixed shape like:
    # {"kibana.alert.threshold_result": {"from": "..."}}
    key_parts = key.split(".")
    for i in range(len(key_parts) - 1, 0, -1):
        prefix = ".".join(key_parts[:i])
        if prefix in source and isinstance(source.get(prefix), dict):
            value: Any = source.get(prefix)
            for suffix_part in key_parts[i:]:
                if isinstance(value, dict):
                    value = value.get(suffix_part)
                else:
                    return None
            return value

    return get_value_by_dot_notation(source, key)


def _is_threshold_rule_source(source: Dict[str, Any]) -> bool:
    rt = _get_alert_field(source, "kibana.alert.rule.type") or _get_alert_field(
        source, "kibana.alert.rule.parameters.type"
    )
    return str(rt).lower() == "threshold"


def _is_esql_rule_source(source: Dict[str, Any]) -> bool:
    rt = _get_alert_field(source, "kibana.alert.rule.type") or _get_alert_field(
        source, "kibana.alert.rule.parameters.type"
    )
    return str(rt).lower() == "esql"


def _is_query_rule_source(source: Dict[str, Any]) -> bool:
    rt = _get_alert_field(source, "kibana.alert.rule.type") or _get_alert_field(
        source, "kibana.alert.rule.parameters.type"
    )
    return str(rt).lower() == "query"


def _is_new_terms_rule_source(source: Dict[str, Any]) -> bool:
    rt = _get_alert_field(source, "kibana.alert.rule.type") or _get_alert_field(
        source, "kibana.alert.rule.parameters.type"
    )
    return str(rt).lower() == "new_terms"


def _is_eql_rule_source(source: Dict[str, Any]) -> bool:
    """SIEM Event Correlation (EQL) rules: ``type`` / ``parameters.type`` ``eql`` (not ES|QL)."""
    rt = _get_alert_field(source, "kibana.alert.rule.type") or _get_alert_field(
        source, "kibana.alert.rule.parameters.type"
    )
    if str(rt).lower() == "eql":
        return True
    rti = _get_alert_field(source, "kibana.alert.rule.rule_type_id")
    return isinstance(rti, str) and "eqlrule" in rti.lower()


def _is_threat_match_rule_source(source: Dict[str, Any]) -> bool:
    """SIEM threat / indicator match rules (``threat_match`` / ``siem.indicatorRule``)."""
    rt = _get_alert_field(source, "kibana.alert.rule.type") or _get_alert_field(
        source, "kibana.alert.rule.parameters.type"
    )
    if str(rt).lower() == "threat_match":
        return True
    rti = _get_alert_field(source, "kibana.alert.rule.rule_type_id")
    return isinstance(rti, str) and "indicatorrule" in rti.lower()


# Sort shape used for SIEM raw-log fetches (query / new_terms / eql).
_SIEM_RAW_LOG_SORT: List[Dict[str, str]] = [{"@timestamp": "asc"}]


def _fetch_progress_print(stage: str, message: str) -> None:
    """Print fetch progress to stdout (visible in script / Prefect runs)."""
    print(f"[{stage}] {message}", flush=True)


def _format_time_range_label(time_range: Any) -> str:
    """Human-readable time range for progress output."""
    if time_range is None:
        return "n/a"
    if isinstance(time_range, dict):
        if "gte" in time_range or "lte" in time_range or "gt" in time_range or "lt" in time_range:
            start = time_range.get("gte") or time_range.get("gt")
            end = time_range.get("lte") or time_range.get("lt")
            return f"start={start!r} end={end!r}"
        if "range" in time_range:
            return _format_time_range_label(time_range["range"])
        for field in ("@timestamp", TIME_FIELD):
            if field in time_range:
                return f"{field} {_format_time_range_label(time_range[field])}"
        return json.dumps(time_range, default=str, separators=(",", ":"))
    return str(time_range)


def _extract_time_range_from_dsl(query_dsl: Any) -> Optional[Dict[str, Any]]:
    """Best-effort extract ``@timestamp`` / ``TIME_FIELD`` range from a query DSL body."""

    def _walk(obj: Any) -> Optional[Dict[str, Any]]:
        if isinstance(obj, dict):
            rng = obj.get("range")
            if isinstance(rng, dict):
                for field in ("@timestamp", TIME_FIELD):
                    bounds = rng.get(field)
                    if isinstance(bounds, dict):
                        return bounds
            for val in obj.values():
                found = _walk(val)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = _walk(item)
                if found is not None:
                    return found
        return None

    return _walk(query_dsl)


def _format_query_for_progress(query: Any) -> str:
    """Compact one-line query text for progress output."""
    if query is None:
        return "n/a"
    if isinstance(query, str):
        return query.strip() or "n/a"
    return json.dumps(query, separators=(",", ":"), default=str)


_QUERY_STRING_QUOTED_SPLIT = re.compile(r'("(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\')')


def _normalize_query_string_operators(query: str) -> str:
    """Uppercase Lucene ``and`` / ``or`` boolean operators outside quoted strings."""
    if not isinstance(query, str) or not query:
        return query
    parts = _QUERY_STRING_QUOTED_SPLIT.split(query)
    normalized: List[str] = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            normalized.append(part)
        else:
            part = re.sub(r"\band\b", "AND", part, flags=re.IGNORECASE)
            part = re.sub(r"\bor\b", "OR", part, flags=re.IGNORECASE)
            normalized.append(part)
    return "".join(normalized)


def _normalize_dsl_query_string_operators(dsl: Any) -> Any:
    """Recursively normalize ``query_string.query`` values in a DSL body."""
    if isinstance(dsl, dict):
        out: Dict[str, Any] = {}
        for key, val in dsl.items():
            if key == "query_string" and isinstance(val, dict):
                qs = dict(val)
                q = qs.get("query")
                if isinstance(q, str):
                    qs["query"] = _normalize_query_string_operators(q)
                out[key] = qs
            else:
                out[key] = _normalize_dsl_query_string_operators(val)
        return out
    if isinstance(dsl, list):
        return [_normalize_dsl_query_string_operators(item) for item in dsl]
    return dsl


def _incident_query_json_string(payload: Any) -> str:
    """One-line JSON for ``incident[\"query\"]`` (compact, no pretty-print)."""
    if isinstance(payload, dict):
        payload = _normalize_dsl_query_string_operators(payload)
    return json.dumps(payload, separators=(",", ":"), default=str)


def _raw_logs_info(
    phase: str,
    rule_type: str,
    fetch_method: str,
    incident: Dict[str, Any],
    **extra: Any,
) -> None:
    """Structured INFO line for raw-log fetch strategy and outcome."""
    parts = [
        "raw_logs:",
        f"phase={phase}",
        f"rule_type={rule_type}",
        f"fetch_method={fetch_method}",
        f"source_id={incident.get('source_id')!r}",
    ]
    for key, val in extra.items():
        if val is None or key in ("query", "indices", "time_range"):
            continue
        parts.append(f"{key}={val}")
    _log().info(" ".join(parts))

    alert_name = incident.get("name") or incident.get("source_id") or "unknown"
    if phase == "execute":
        query = extra.get("query")
        indices = extra.get("indices")
        time_range = extra.get("time_range")
        if time_range is None and isinstance(query, dict):
            time_range = _extract_time_range_from_dsl(query)
        query_text = _format_query_for_progress(query if query is not None else incident.get("query"))
        indices_text = json.dumps(indices, default=str) if indices is not None else "n/a"
        time_range_text = _format_time_range_label(time_range)
        _fetch_progress_print(
            "raw_logs",
            f"fetching alert={alert_name!r} rule_type={rule_type} "
            f"method={fetch_method} indices={indices_text}\n"
            f"  time_range: {time_range_text}\n"
            f"  query: {query_text}",
        )
    elif phase == "done":
        _fetch_progress_print(
            "raw_logs",
            f"done alert={alert_name!r} rule_type={rule_type} "
            f"raw_logs_count={extra.get('raw_logs_count', 0)}",
        )
    elif phase == "failed":
        _fetch_progress_print(
            "raw_logs",
            f"failed alert={alert_name!r} rule_type={rule_type} "
            f"error={extra.get('error')!r}",
        )


def _alert_ancestors_indices_and_ids_dsl(
    source: Dict[str, Any],
) -> Optional[Tuple[List[str], Dict[str, Any]]]:
    """First ``kibana.alert.ancestors`` row with ``index`` + ``id`` -> ``ids`` query."""
    ancestors = _get_alert_field(source, "kibana.alert.ancestors")
    if not isinstance(ancestors, list):
        return None
    for row in ancestors:
        if not isinstance(row, dict):
            continue
        idx = row.get("index")
        doc_id = row.get("id")
        if not (
            isinstance(idx, str)
            and idx.strip()
            and isinstance(doc_id, str)
            and doc_id.strip()
        ):
            continue
        query_dsl: Dict[str, Any] = {
            "query": {"ids": {"values": [doc_id.strip()]}},
            "sort": _SIEM_RAW_LOG_SORT,
        }
        return [idx.strip()], query_dsl
    return None


def _rule_indices_for_raw_logs(source: Dict[str, Any]) -> List[str]:
    """Resolve rule ``index`` / ``indices`` to a non-empty list of index patterns."""
    raw = _get_alert_field(source, "kibana.alert.rule.parameters.index") or _get_alert_field(
        source, "kibana.alert.rule.indices"
    )
    if isinstance(raw, str) and raw.strip():
        return [raw.strip()]
    if isinstance(raw, list) and raw:
        return [str(x).strip() for x in raw if str(x).strip()]
    return [FETCH_INDEX]


def _build_query_rule_raw_logs_query(
    source: Dict[str, Any],
) -> Optional[Tuple[List[str], Dict[str, Any], str]]:
    """Return ``(indices, query_dsl, fetch_method)`` for query-rule raw logs, or ``None``.

    Prefer ``kibana.alert.ancestors`` (``ids`` on backing index). Else ``bool`` with
    ``must.query_string`` (rule text as-is), ``filter`` = ``@timestamp`` range (plus
    rule ``filters`` when present). Sort ``_SIEM_RAW_LOG_SORT``; size from
    ``RAW_LOGS_FETCH_SIZE`` in ``execute_raw_query``. No aggregations.
    """
    if not _is_query_rule_source(source):
        return None

    backing = _alert_ancestors_indices_and_ids_dsl(source)
    if backing is not None:
        indices, query_dsl = backing
        _log().debug(
            "Query-rule raw_logs: ancestors ids query "
            f"indices={indices!r} dsl_keys={list(query_dsl.keys())}"
        )
        return indices, query_dsl, "ancestors_ids"

    rule_query = _get_alert_field(source, "kibana.alert.rule.parameters.query")
    if not rule_query or not str(rule_query).strip():
        _log().debug("Query-rule raw_logs: missing parameters.query")
        return None

    intended = _alert_intended_timestamp_utc(source)
    if intended is None:
        _log().debug("Query-rule raw_logs: missing kibana.alert.intended_timestamp")
        return None

    rule_from = _get_alert_field(source, "kibana.alert.rule.parameters.from") or _get_alert_field(
        source, "kibana.alert.rule.from"
    )
    start_dt = _esql_window_start_from_rule_from(rule_from, intended)
    if start_dt is None:
        _log().debug(f"Query-rule raw_logs: could not parse rule.from={rule_from!r}")
        return None

    start_iso = _format_utc_iso_z(start_dt)
    end_iso = _format_utc_iso_z(intended)
    rule_filters = _get_alert_field(source, "kibana.alert.rule.parameters.filters") or []

    range_clause: Dict[str, Any] = {
        "range": {"@timestamp": {"gte": start_iso, "lte": end_iso}},
    }
    extra_filters = _extract_threshold_query_filters(rule_filters)
    if extra_filters:
        filter_part: Any = [range_clause] + extra_filters
    else:
        filter_part = range_clause

    bool_query: Dict[str, Any] = {
        "must": {"query_string": {"query": str(rule_query)}},
        "filter": filter_part,
    }

    indices = _rule_indices_for_raw_logs(source)
    query_dsl: Dict[str, Any] = {
        "query": {"bool": bool_query},
        "sort": _SIEM_RAW_LOG_SORT,
    }
    _log().debug(
        "Query-rule raw_logs DSL: "
        f"indices={indices}, window=({start_iso!r}, {end_iso!r})"
    )
    return indices, query_dsl, "bool_query_time_window"


def _new_terms_field_value_pairs(source: Dict[str, Any]) -> List[Tuple[str, Any]]:
    """Pair ``parameters.new_terms_fields`` with ``kibana.alert.new_terms`` values."""
    fields = _get_alert_field(source, "kibana.alert.rule.parameters.new_terms_fields")
    values = _get_alert_field(source, "kibana.alert.new_terms")
    if not isinstance(fields, list) or not fields:
        return []
    if isinstance(values, list):
        val_list = values
    elif values is None or values == "":
        val_list = []
    else:
        val_list = [values]
    pairs: List[Tuple[str, Any]] = []
    for i, field in enumerate(fields):
        if not isinstance(field, str) or not field.strip():
            continue
        v = val_list[i] if i < len(val_list) else None
        if v is None or v == "":
            continue
        pairs.append((field.strip(), v))
    return pairs


def _build_new_terms_raw_logs_query(
    source: Dict[str, Any],
) -> Optional[Tuple[List[str], Dict[str, Any], str]]:
    """Return ``(indices, query_dsl, fetch_method)`` for new_terms-rule raw logs, or ``None``.

    Prefer ``kibana.alert.ancestors`` (``ids``). Else ``bool`` with ``must.query_string``
    (rule query), ``filter`` = ``@timestamp`` range + ``term`` per new term field/value
    + optional rule ``filters``. Same sort/size pattern as query-rule fallback.
    """
    if not _is_new_terms_rule_source(source):
        return None

    backing = _alert_ancestors_indices_and_ids_dsl(source)
    if backing is not None:
        _log().debug(
            "New-terms raw_logs: ancestors ids query "
            f"indices={backing[0]!r}"
        )
        indices, query_dsl = backing
        return indices, query_dsl, "ancestors_ids"

    rule_query = _get_alert_field(source, "kibana.alert.rule.parameters.query")
    if not rule_query or not str(rule_query).strip():
        _log().debug("New-terms raw_logs: missing parameters.query")
        return None

    intended = _alert_intended_timestamp_utc(source)
    if intended is None:
        _log().debug("New-terms raw_logs: missing kibana.alert.intended_timestamp")
        return None

    rule_from = _get_alert_field(source, "kibana.alert.rule.parameters.from") or _get_alert_field(
        source, "kibana.alert.rule.from"
    )
    start_dt = _esql_window_start_from_rule_from(rule_from, intended)
    if start_dt is None:
        _log().debug(f"New-terms raw_logs: could not parse rule.from={rule_from!r}")
        return None

    start_iso = _format_utc_iso_z(start_dt)
    end_iso = _format_utc_iso_z(intended)
    rule_filters = _get_alert_field(source, "kibana.alert.rule.parameters.filters") or []

    range_clause: Dict[str, Any] = {
        "range": {"@timestamp": {"gte": start_iso, "lte": end_iso}},
    }
    entity_pairs = _new_terms_field_value_pairs(source)
    filter_clauses: List[Any] = [range_clause]
    for field, val in entity_pairs:
        filter_clauses.append({"term": {field: val}})
    filter_clauses.extend(_extract_threshold_query_filters(rule_filters))

    bool_query: Dict[str, Any] = {
        "must": {"query_string": {"query": str(rule_query).strip()}},
        "filter": filter_clauses,
    }

    indices = _rule_indices_for_raw_logs(source)
    query_dsl: Dict[str, Any] = {
        "query": {"bool": bool_query},
        "sort": _SIEM_RAW_LOG_SORT,
    }
    _log().debug(
        "New-terms raw_logs fallback: "
        f"indices={indices!r} window=({start_iso!r}, {end_iso!r}) "
        f"entity_terms={len(entity_pairs)}"
    )
    return indices, query_dsl, "bool_query_time_window"


def _build_eql_raw_logs_query(
    source: Dict[str, Any],
) -> Optional[Tuple[List[str], Dict[str, Any], str]]:
    """Return ``(indices, query_dsl, fetch_method)`` for SIEM EQL-rule raw logs, or ``None``.

    Prefer ``kibana.alert.ancestors`` (``ids``). Else ``bool`` with ``must.query_string``
    (rule ``parameters.query`` as plain string), ``filter`` = ``@timestamp`` range (single
    object when there are no extra rule filters) + optional ``parameters.filters``.
    No aggregations; same sort as query/new_terms.
    """
    if not _is_eql_rule_source(source):
        return None

    backing = _alert_ancestors_indices_and_ids_dsl(source)
    if backing is not None:
        indices, query_dsl = backing
        _log().debug(
            "EQL-rule raw_logs: ancestors ids query "
            f"indices={indices!r} dsl_keys={list(query_dsl.keys())}"
        )
        return indices, query_dsl, "ancestors_ids"

    rule_query = _get_alert_field(source, "kibana.alert.rule.parameters.query")
    if not rule_query or not str(rule_query).strip():
        _log().debug("EQL-rule raw_logs: missing parameters.query")
        return None

    intended = _alert_intended_timestamp_utc(source)
    if intended is None:
        _log().debug("EQL-rule raw_logs: missing kibana.alert.intended_timestamp")
        return None

    rule_from = _get_alert_field(source, "kibana.alert.rule.parameters.from") or _get_alert_field(
        source, "kibana.alert.rule.from"
    )
    start_dt = _esql_window_start_from_rule_from(rule_from, intended)
    if start_dt is None:
        _log().debug(f"EQL-rule raw_logs: could not parse rule.from={rule_from!r}")
        return None

    start_iso = _format_utc_iso_z(start_dt)
    end_iso = _format_utc_iso_z(intended)
    rule_filters = _get_alert_field(source, "kibana.alert.rule.parameters.filters") or []

    range_clause: Dict[str, Any] = {
        "range": {"@timestamp": {"gte": start_iso, "lte": end_iso}},
    }
    extra_filters = _extract_threshold_query_filters(rule_filters)
    if extra_filters:
        filter_part: Any = [range_clause] + extra_filters
    else:
        filter_part = range_clause

    bool_query: Dict[str, Any] = {
        "must": {"query_string": {"query": str(rule_query).strip()}},
        "filter": filter_part,
    }

    indices = _rule_indices_for_raw_logs(source)
    query_dsl: Dict[str, Any] = {
        "query": {"bool": bool_query},
        "sort": _SIEM_RAW_LOG_SORT,
    }
    _log().debug(
        "EQL-rule raw_logs fallback DSL: "
        f"indices={indices}, window=({start_iso!r}, {end_iso!r})"
    )
    return indices, query_dsl, "bool_query_time_window"


def _build_threat_match_raw_logs_query(
    source: Dict[str, Any],
) -> Optional[Tuple[List[str], Dict[str, Any], str]]:
    """Return ``(indices, query_dsl, fetch_method)`` for threat_match alerts, or ``None``.

    Only ``kibana.alert.ancestors`` (``ids`` on the backing event index). No fallback:
    indicator rules join threat indices and are not reproduced with a single
    ``query_string`` + time window.
    """
    if not _is_threat_match_rule_source(source):
        return None
    backing = _alert_ancestors_indices_and_ids_dsl(source)
    if backing is None:
        _log().debug(
            "Threat-match raw_logs: no usable kibana.alert.ancestors "
            f"rule_uuid={_get_alert_field(source, 'kibana.alert.rule.uuid')!r}"
        )
        return None
    indices, query_dsl = backing
    _log().debug(f"Threat-match raw_logs: ancestors ids query indices={indices!r}")
    return indices, query_dsl, "ancestors_ids"


def _append_raw_logs_event_original_from_hits(incident: Dict[str, Any], hits: Any) -> None:
    """Append ``event.original`` strings from search hits to ``incident[\"raw_logs\"]``."""
    if not isinstance(hits, list):
        return
    for raw_hit in hits:
        if not isinstance(raw_hit, dict):
            continue
        raw_source = raw_hit.get("_source")
        if not isinstance(raw_source, dict):
            continue
        event_data = raw_source.get("event")
        if not isinstance(event_data, dict):
            continue
        event_original = event_data.get("original")
        if event_original is not None:
            incident["raw_logs"].append(str(event_original))


def _enrich_incident_with_query_rule_raw_logs(
    incident: Dict[str, Any], es: Any, source: Optional[Dict[str, Any]] = None
) -> None:
    """Populate ``raw_logs`` / ``query`` for SIEM query-rule alerts."""
    source = source or {}
    if not isinstance(source, dict) or not source:
        return

    payload = _build_query_rule_raw_logs_query(source)
    if not payload:
        return

    indices, query_dsl, fetch_method = payload
    incident["raw_logs"] = []
    incident["query"] = _incident_query_json_string(query_dsl)
    _raw_logs_info("execute", "query", fetch_method, incident, query=query_dsl, indices=indices)
    try:
        response = execute_raw_query(
            es, query_dsl, index=indices, size=RAW_LOGS_FETCH_SIZE, page=0
        )
        hits = response.get("hits", {}).get("hits", [])
        _append_raw_logs_event_original_from_hits(incident, hits)
        n_raw = len(incident["raw_logs"])
        _raw_logs_info(
            "done",
            "query",
            fetch_method,
            incident,
            es_hits=len(hits) if isinstance(hits, list) else 0,
            raw_logs_count=n_raw,
            hits_found=n_raw > 0,
        )
        _log().debug(
            f"Query-rule raw_logs: done source_id={incident.get('source_id')!r} "
            f"n={n_raw}"
        )
    except Exception as ex:
        incident["raw_logs_error"] = str(ex)
        _raw_logs_info("failed", "query", fetch_method, incident, error=str(ex))
        _log().debug(
            f"Query-rule raw_logs: failed source_id={incident.get('source_id')!r} error={ex}"
        )


def _enrich_incident_with_new_terms_raw_logs(
    incident: Dict[str, Any], es: Any, source: Optional[Dict[str, Any]] = None
) -> None:
    """Populate ``raw_logs`` / ``query`` for SIEM new_terms-rule alerts."""
    source = source or {}
    if not isinstance(source, dict) or not source:
        return

    payload = _build_new_terms_raw_logs_query(source)
    if not payload:
        return

    indices, query_dsl, fetch_method = payload
    incident["raw_logs"] = []
    incident["query"] = _incident_query_json_string(query_dsl)
    _raw_logs_info("execute", "new_terms", fetch_method, incident, query=query_dsl, indices=indices)
    try:
        response = execute_raw_query(
            es, query_dsl, index=indices, size=RAW_LOGS_FETCH_SIZE, page=0
        )
        hits = response.get("hits", {}).get("hits", [])
        _append_raw_logs_event_original_from_hits(incident, hits)
        n_raw = len(incident["raw_logs"])
        _raw_logs_info(
            "done",
            "new_terms",
            fetch_method,
            incident,
            es_hits=len(hits) if isinstance(hits, list) else 0,
            raw_logs_count=n_raw,
            hits_found=n_raw > 0,
        )
        _log().debug(
            f"New-terms raw_logs: done source_id={incident.get('source_id')!r} "
            f"n={n_raw}"
        )
    except Exception as ex:
        incident["raw_logs_error"] = str(ex)
        _raw_logs_info("failed", "new_terms", fetch_method, incident, error=str(ex))
        _log().debug(
            f"New-terms raw_logs: failed source_id={incident.get('source_id')!r} error={ex}"
        )


def _enrich_incident_with_eql_rule_raw_logs(
    incident: Dict[str, Any], es: Any, source: Optional[Dict[str, Any]] = None
) -> None:
    """Populate ``raw_logs`` / ``query`` for SIEM EQL-rule alerts."""
    source = source or {}
    if not isinstance(source, dict) or not source:
        return

    payload = _build_eql_raw_logs_query(source)
    if not payload:
        return

    indices, query_dsl, fetch_method = payload
    incident["raw_logs"] = []
    incident["query"] = _incident_query_json_string(query_dsl)
    _raw_logs_info("execute", "eql", fetch_method, incident, query=query_dsl, indices=indices)
    try:
        response = execute_raw_query(
            es, query_dsl, index=indices, size=RAW_LOGS_FETCH_SIZE, page=0
        )
        hits = response.get("hits", {}).get("hits", [])
        _append_raw_logs_event_original_from_hits(incident, hits)
        n_raw = len(incident["raw_logs"])
        _raw_logs_info(
            "done",
            "eql",
            fetch_method,
            incident,
            es_hits=len(hits) if isinstance(hits, list) else 0,
            raw_logs_count=n_raw,
            hits_found=n_raw > 0,
        )
        _log().debug(
            f"EQL-rule raw_logs: done source_id={incident.get('source_id')!r} "
            f"n={n_raw}"
        )
    except Exception as ex:
        incident["raw_logs_error"] = str(ex)
        _raw_logs_info("failed", "eql", fetch_method, incident, error=str(ex))
        _log().debug(
            f"EQL-rule raw_logs: failed source_id={incident.get('source_id')!r} error={ex}"
        )


def _enrich_incident_with_threat_match_raw_logs(
    incident: Dict[str, Any], es: Any, source: Optional[Dict[str, Any]] = None
) -> None:
    """Populate ``raw_logs`` / ``query`` for SIEM threat_match (indicator) alerts via ancestors only."""
    source = source or {}
    if not isinstance(source, dict) or not source:
        return

    payload = _build_threat_match_raw_logs_query(source)
    if not payload:
        return

    indices, query_dsl, fetch_method = payload
    incident["raw_logs"] = []
    incident["query"] = _incident_query_json_string(query_dsl)
    _raw_logs_info("execute", "threat_match", fetch_method, incident, query=query_dsl, indices=indices)
    try:
        response = execute_raw_query(
            es, query_dsl, index=indices, size=RAW_LOGS_FETCH_SIZE, page=0
        )
        hits = response.get("hits", {}).get("hits", [])
        _append_raw_logs_event_original_from_hits(incident, hits)
        n_raw = len(incident["raw_logs"])
        _raw_logs_info(
            "done",
            "threat_match",
            fetch_method,
            incident,
            es_hits=len(hits) if isinstance(hits, list) else 0,
            raw_logs_count=n_raw,
            hits_found=n_raw > 0,
        )
        _log().debug(
            f"Threat-match raw_logs: done source_id={incident.get('source_id')!r} "
            f"n={n_raw}"
        )
    except Exception as ex:
        incident["raw_logs_error"] = str(ex)
        _raw_logs_info("failed", "threat_match", fetch_method, incident, error=str(ex))
        _log().debug(
            f"Threat-match raw_logs: failed source_id={incident.get('source_id')!r} error={ex}"
        )


def _alert_intended_timestamp_utc(source: Dict[str, Any]) -> Optional[datetime]:
    """Parse ``kibana.alert.intended_timestamp`` to aware UTC."""
    raw = _get_alert_field(source, "kibana.alert.intended_timestamp")
    if not raw:
        return None
    try:
        dt = parse(str(raw))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def _esql_window_start_from_rule_from(rule_from: Any, intended_end: datetime) -> Optional[datetime]:
    """Lower bound = ``intended_end`` minus lookback encoded in ``rule.from`` (e.g. ``now-9m``).

    Anchors Kibana-style ``now-...`` to ``intended_end`` (UTC), not local wall clock.
    """
    if not rule_from or not isinstance(rule_from, str):
        return None
    base = intended_end.astimezone(timezone.utc).replace(tzinfo=None)
    parsed = dateparser.parse(
        rule_from.strip(),
        settings={
            "RELATIVE_BASE": base,
            "TIMEZONE": "UTC",
            "PREFER_DATES_FROM": "past",
        },
    )
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _esql_split_pipeline(esql: str) -> List[str]:
    """Split ES|QL into command segments (first segment is usually ``FROM ...``)."""
    t = esql.strip()
    if not t:
        return []
    parts = re.split(r"\r?\n\s*\|\s*|\s*\|\s*", t)
    return [p.strip() for p in parts if p.strip()]


def _esql_join_pipeline(segments: List[str]) -> str:
    if not segments:
        return ""
    lines = [segments[0]]
    for seg in segments[1:]:
        lines.append("| " + seg)
    return "\n".join(lines)


def _esql_should_drop_raw_logs_segment(seg: str) -> bool:
    """Drop eval date_trunc, stats, and post-aggregation threshold ``where`` clauses."""
    s = seg.strip()
    if not s:
        return True

    # Ignore leading comment lines so command detection works on commented pipelines.
    non_comment_lines = [
        ln.strip()
        for ln in s.splitlines()
        if ln.strip() and not ln.strip().startswith("//")
    ]
    cmd = "\n".join(non_comment_lines).strip() if non_comment_lines else ""
    low = cmd.lower()

    if not cmd:
        # Segment is only comments/whitespace.
        return True
    if low.startswith("eval ") and "date_trunc" in low:
        return True
    if low.startswith("stats "):
        return True
    if low.startswith("where "):
        if "esql." in low and re.search(r"[><]=?|!=|==", low):
            return True
        if re.search(r"\bfail_count\s*[><]=?\s*\d", low):
            return True
    return False


def _esql_keep_strip_esql_fields(seg: str) -> Optional[str]:
    """Remove ``Esql.*`` field names from a ``keep`` command; ``None`` if nothing left."""
    s = seg.strip()
    non_comment_lines = [
        ln.strip()
        for ln in s.splitlines()
        if ln.strip() and not ln.strip().startswith("//")
    ]
    cleaned_cmd = "\n".join(non_comment_lines).strip()
    m = re.match(r"(?i)^keep\s+(.+)$", cleaned_cmd, re.DOTALL)
    if not m:
        return seg
    body = re.sub(r"\s+", " ", m.group(1).replace("\n", " ")).strip()
    parts = re.split(r"\s*,\s*", body)
    kept: List[str] = []
    for p in parts:
        p = p.strip()
        if not p or re.match(r"(?i)^Esql\.", p):
            continue
        kept.append(p)
    if not kept:
        return None
    return "keep " + ", ".join(kept)


def _esql_rhs_literal(val: Any) -> str:
    """Right-hand side for ES|QL ``field == ...``."""
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return str(int(val)) if isinstance(val, float) and val == int(val) else str(val)
    s = str(val).strip()
    esc = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{esc}"'


def _esql_stats_by_fields(esql: str) -> List[str]:
    """Extract ``STATS ... BY <fields>`` grouping keys from ES|QL."""
    m = re.search(r"(?is)\bstats\b[\s\S]*?\bby\b([\s\S]*?)(?:\|\s*|$)", esql)
    if not m:
        return []
    by_clause = re.sub(r"(?m)^\s*//.*$", "", m.group(1)).strip()
    if not by_clause:
        return []
    tokens = [t.strip() for t in re.split(r"\s*,\s*", by_clause) if t.strip()]
    fields: List[str] = []
    for tok in tokens:
        # Keep plain field refs only; skip aliases/functions/derived expressions.
        if re.search(r"[()=+*/-]", tok):
            continue
        if tok.strip().lower() in ("window", "time_window", "bucket", "time_bucket"):
            continue
        if re.match(r"(?i)^Esql\.", tok):
            continue
        fields.append(tok)
    return fields


def _esql_entity_where_clause(
    source: Dict[str, Any], by_fields: Optional[List[str]] = None
) -> Optional[str]:
    """Build ``where`` clause from alert values.

    For aggregation rules, prefer dynamic ``STATS BY`` fields from the rule.
    """
    clauses: List[str] = []
    if by_fields:
        for field in by_fields:
            val = _get_alert_field(source, field)
            if val is None or val == "":
                continue
            clauses.append(f"{field} == {_esql_rhs_literal(val)}")
        if clauses:
            return "where " + " AND ".join(clauses)

    # Fallback heuristics for non-aggregation or missing-by-values cases.
    for field in (
        "source.ip",
        "destination.ip",
        "client.ip",
        "source.user.name",
        "user.name",
        "host.name",
    ):
        val = _get_alert_field(source, field)
        if val is None or val == "":
            continue
        return f"where {field} == {_esql_rhs_literal(val)}"
    return None


def _esql_uses_aggregation(esql: str) -> bool:
    """Return ``True`` when ES|QL appears aggregation-based (stats/bucket-style)."""
    low = esql.lower()
    return ("| stats" in low) or ("stats " in low)


def _simplify_esql_for_raw_event_rows(
    esql: str, source: Dict[str, Any]
) -> Optional[Tuple[str, str]]:
    """Strip aggregation/threshold stages; narrow to entity on alert; keep row-level pipeline.

    Returns ``(pipeline_string, entity_where_clause)`` or ``None``.
    """
    by_fields = _esql_stats_by_fields(esql)
    entity = _esql_entity_where_clause(source, by_fields=by_fields)

    segments = _esql_split_pipeline(esql)
    if not segments:
        return None

    out: List[str] = []
    for seg in segments:
        if _esql_should_drop_raw_logs_segment(seg):
            continue
        low = seg.strip().lower()
        if low.startswith("keep "):
            cleaned = _esql_keep_strip_esql_fields(seg)
            if cleaned is None:
                continue
            seg = cleaned
        out.append(seg)

    # Apply entity narrowing only for aggregation-style detections.
    # For row-level/non-aggregated rules, forcing ``source.ip`` can be arbitrary.
    if _esql_uses_aggregation(esql):
        if not entity:
            _log().debug(
                "ES|QL raw_logs: aggregation rule but no STATS BY fields found on alert values. "
                f"by_fields={by_fields!r}"
            )
            return None
        out.append(entity)
        return _esql_join_pipeline(out), entity

    return _esql_join_pipeline(out), ""


def _append_esql_sort_and_limit(esql: str, limit: int) -> str:
    """Append ``SORT @timestamp ASC`` and ``LIMIT``; strip any trailing sort/limit first."""
    q = esql.rstrip()
    q = re.sub(r"\|\s*LIMIT\s+\d+\s*$", "", q, flags=re.IGNORECASE | re.MULTILINE)
    q = re.sub(
        r"\|\s*SORT\s+@timestamp\s+ASC\s*$",
        "",
        q,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    q = q.rstrip()
    return f"{q}\n| SORT @timestamp ASC\n| LIMIT {int(limit)}"


def _finalize_esql_raw_logs_query(esql: str) -> str:
    """Hard cleanup pass to ensure aggregation-era clauses are removed.

    This is a safety net for multiline/comment-heavy ES|QL where segment parsing can miss
    a command boundary.
    """
    q = esql
    # Remove any residual stats stage.
    q = re.sub(
        r"\|\s*stats\b[\s\S]*?(?=\n\s*\||\Z)",
        "",
        q,
        flags=re.IGNORECASE,
    )
    # Remove where clauses that reference computed Esql.* fields.
    q = re.sub(
        r"\|\s*where\b[\s\S]*?\bEsql\.[\s\S]*?(?=\n\s*\||\Z)",
        "",
        q,
        flags=re.IGNORECASE,
    )
    # Remove explicit Esql.* projection lines if they survived in keep lists.
    q = re.sub(
        r"(?im)^\s*Esql\.[^,\n]*,?\s*$",
        "",
        q,
    )
    # Normalize accidental duplicate blank lines.
    q = re.sub(r"\n{3,}", "\n\n", q).strip()
    return q


def _inject_esql_timestamp_window(esql: str, start_iso: str, end_iso: str) -> str:
    """Insert ``| WHERE @timestamp`` window immediately after the first FROM clause."""
    text = esql.strip()
    if not text:
        return text
    first_pipe = text.find("|")
    if first_pipe == -1:
        prefix = text
        suffix = ""
    else:
        prefix = text[:first_pipe].strip()
        suffix = text[first_pipe:].lstrip()  # rest starts with "|"
    window = (
        f'| WHERE @timestamp >= TO_DATETIME("{start_iso}") '
        f'AND @timestamp <= TO_DATETIME("{end_iso}")'
    )
    if suffix:
        return f"{prefix}\n{window}\n{suffix}"
    return f"{prefix}\n{window}"


def _perform_esql_json(
    es: Any, esql_query: str, filter_dsl: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Run ES|QL via ``/_query``; requires Elasticsearch v8/v9 client."""
    if ELASTIC_SEARCH_CLIENT not in (ELASTICSEARCH_V8, ELASTICSEARCH_V9):
        raise DemistoException(
            "ES|QL raw_logs enrichment requires client_type Elasticsearch_v8 or Elasticsearch_v9."
        )
    compatible_with = 8 if ELASTIC_SEARCH_CLIENT == ELASTICSEARCH_V8 else 9
    headers = {
        "Content-Type": (
            f"application/vnd.elasticsearch+json; compatible-with={compatible_with}"
        ),
        "Accept": (
            f"application/vnd.elasticsearch+json; compatible-with={compatible_with}"
        ),
    }
    body: Dict[str, Any] = {"query": esql_query}
    if isinstance(filter_dsl, dict) and filter_dsl:
        body["filter"] = filter_dsl
    res = es.perform_request(
        method="POST",
        path="/_query?format=json",
        headers=headers,
        body=body,
    )
    payload = getattr(res, "body", res)
    if not isinstance(payload, dict):
        raise DemistoException(f"Unexpected ES|QL response type: {type(payload)!r}")
    return payload


def _esql_json_to_row_dicts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Turn ES|QL JSON ``columns`` + ``values`` into list of row dicts."""
    columns = payload.get("columns") or []
    values = payload.get("values") or []
    names = [c.get("name") for c in columns if isinstance(c, dict)]
    rows: List[Dict[str, Any]] = []
    for row in values:
        if not isinstance(row, (list, tuple)):
            continue
        row_dict: Dict[str, Any] = {}
        for i, name in enumerate(names):
            if name and i < len(row):
                row_dict[str(name)] = row[i]
        rows.append(row_dict)
    return rows


def _row_dict_to_event_original_value(row: Dict[str, Any]) -> str:
    """Extract a displayable raw log string from one ES|QL row."""
    for key in ("event.original",):
        val = row.get(key)
        if val is not None and val != "":
            if not isinstance(val, str):
                val = json.dumps(val, default=str)
            return val
    for k, v in row.items():
        if v is None or v == "":
            continue
        if "original" in str(k).lower():
            if not isinstance(v, str):
                v = json.dumps(v, default=str)
            return v
    return json.dumps(row, default=str)


def _enrich_incident_with_esql_raw_logs(
    incident: Dict[str, Any], es: Any, source: Dict[str, Any]
) -> None:
    """Run rule ES|QL over [@timestamp start, intended] and attach rows to ``raw_logs``."""
    if not _is_esql_rule_source(source):
        return

    intended = _alert_intended_timestamp_utc(source)
    rule_from = _get_alert_field(source, "kibana.alert.rule.from") or _get_alert_field(
        source, "kibana.alert.rule.parameters.from"
    )
    base_esql = _get_alert_field(source, "kibana.alert.rule.parameters.query")
    if not isinstance(base_esql, str) or not base_esql.strip():
        _log().debug("ES|QL raw_logs: missing parameters.query")
        return
    if intended is None:
        _log().debug("ES|QL raw_logs: missing kibana.alert.intended_timestamp")
        return

    _log().info(
        "kibana.alert.rule.parameters.query (before construction): "
        f"rule_uuid={_get_alert_field(source, 'kibana.alert.rule.uuid')!r}, "
        f"query={base_esql!r}"
    )

    start_dt = _esql_window_start_from_rule_from(rule_from, intended)
    if start_dt is None:
        _log().debug(f"ES|QL raw_logs: could not parse rule.from={rule_from!r}")
        return

    start_iso = _format_utc_iso_z(start_dt)
    end_iso = _format_utc_iso_z(intended)

    simplified = _simplify_esql_for_raw_event_rows(base_esql, source)
    if simplified is None:
        return
    pipeline_body, entity_where = simplified

    modified = _finalize_esql_raw_logs_query(pipeline_body)
    modified = _append_esql_sort_and_limit(modified, RAW_LOGS_FETCH_SIZE)
    time_filter = {
        "range": {
            "@timestamp": {
                "gte": start_iso,
                "lte": end_iso,
            }
        }
    }

    query_record: Dict[str, Any] = {
        "type": "esql",
        "time_window": {"gte": start_iso, "lte": end_iso},
        "rule_from": rule_from,
        "query": modified,
        "filter": time_filter,
    }
    if entity_where:
        query_record["entity_where"] = entity_where
    incident["query"] = _incident_query_json_string(query_record)
    incident.setdefault("raw_logs", [])
    incident["raw_logs"] = []

    fetch_method = "esql_post_query"
    _raw_logs_info(
        "execute",
        "esql",
        fetch_method,
        incident,
        query=modified,
        time_range={"gte": start_iso, "lte": end_iso},
    )

    try:
        payload = _perform_esql_json(es, modified, filter_dsl=time_filter)
        if payload.get("error"):
            raise DemistoException(str(payload.get("error")))
        rows = _esql_json_to_row_dicts(payload)
        for row in rows[:RAW_LOGS_FETCH_SIZE]:
            incident["raw_logs"].append(_row_dict_to_event_original_value(row))
        n_raw = len(incident["raw_logs"])
        _raw_logs_info(
            "done",
            "esql",
            fetch_method,
            incident,
            esql_rows=len(rows),
            raw_logs_count=n_raw,
            hits_found=n_raw > 0,
        )
        _log().debug(
            f"ES|QL raw_logs: done source_id={incident.get('source_id')!r} "
            f"rows={n_raw}"
        )
    except Exception as ex:
        incident["raw_logs_error"] = str(ex)
        _raw_logs_info("failed", "esql", fetch_method, incident, error=str(ex))
        _log().debug(
            f"ES|QL raw_logs: failed source_id={incident.get('source_id')!r} error={ex}"
        )


def _build_threshold_raw_logs_query(
    source: Dict[str, Any],
) -> Optional[Tuple[List[str], Dict[str, Any], str]]:
    """Build ``(indices, query_dsl, fetch_method)`` for fetching raw logs of one threshold alert."""
    rule_type = _get_alert_field(source, "kibana.alert.rule.type") or _get_alert_field(
        source, "kibana.alert.rule.parameters.type"
    )
    _log().debug(f"Threshold raw_logs builder: evaluating rule_type={rule_type!r}")
    if str(rule_type).lower() != "threshold":
        _log().debug("Threshold raw_logs builder: skipping non-threshold alert.")
        return None

    rule_query = _get_alert_field(source, "kibana.alert.rule.parameters.query")
    rule_filters = _get_alert_field(source, "kibana.alert.rule.parameters.filters") or []
    threshold_terms = _get_alert_field(source, "kibana.alert.threshold_result.terms") or []
    _log().info(
        "kibana.alert.rule.parameters.query (before construction): "
        f"rule_uuid={_get_alert_field(source, 'kibana.alert.rule.uuid')!r}, "
        f"query={rule_query!r}"
    )
    # Use the threshold incident window from the signal itself.
    start_time = _get_alert_field(source, "kibana.alert.threshold_result.from")
    end_time = _get_alert_field(source, "kibana.alert.intended_timestamp")

    if not start_time or not end_time:
        _log().debug(
            "Threshold raw_logs builder: missing time bounds. "
            f"start_time={start_time!r}, end_time={end_time!r}"
        )
        return None

    bool_query: Dict[str, List[Dict[str, Any]]] = {
        "filter": [{"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}]
    }
    if rule_query:
        bool_query["must"] = [{"query_string": {"query": str(rule_query)}}]

    bool_query["filter"].extend(_extract_threshold_query_filters(rule_filters))

    if isinstance(threshold_terms, list):
        for term in threshold_terms:
            if not isinstance(term, dict):
                continue
            field = term.get("field")
            value = term.get("value")
            if field and value is not None:
                bool_query["filter"].append({"term": {str(field): value}})

    indices = _get_alert_field(source, "kibana.alert.rule.parameters.index") or _get_alert_field(
        source, "kibana.alert.rule.indices"
    ) or []
    if not isinstance(indices, list) or not indices:
        indices = [FETCH_INDEX]

    query_dsl = {
        "query": {"bool": bool_query},
        "sort": [{"@timestamp": {"order": "asc"}}],
    }
    _log().debug(
        "Threshold raw_logs builder: built query payload. "
        f"indices={indices}, terms_count={len(threshold_terms) if isinstance(threshold_terms, list) else 0}, "
        f"filters_count={len(rule_filters) if isinstance(rule_filters, list) else 0}"
    )
    _log().debug(f"Threshold raw_logs builder DSL: {json.dumps(query_dsl)}")
    return indices, query_dsl, "threshold_bool_dsl"


def _enrich_incident_with_threshold_raw_logs(
    incident: Dict[str, Any], es: Any, source: Optional[Dict[str, Any]] = None
) -> None:
    """Fetch backing raw logs for threshold incidents; set ``raw_logs`` and ``query``."""
    _log().debug(
        "Threshold raw_logs enrichment: "
        f"name={incident.get('name')!r}, source_id={incident.get('source_id')!r}"
    )
    source = source or {}
    if not isinstance(source, dict) or not source:
        _log().debug("Threshold enrichment: source missing/invalid.")
        return

    # Ensure key always exists for threshold incidents processing visibility.
    query_payload = _build_threshold_raw_logs_query(source)
    if not query_payload:
        _log().debug(
            "Threshold raw_logs: skip (missing threshold fields): "
            f"source_id={incident.get('source_id')!r}, "
            f"from={_get_alert_field(source, 'kibana.alert.threshold_result.from')!r}, "
            f"intended={_get_alert_field(source, 'kibana.alert.intended_timestamp')!r}"
        )
        return

    indices, query_dsl, fetch_method = query_payload
    incident["raw_logs"] = []
    incident["query"] = _incident_query_json_string(query_dsl)
    _raw_logs_info("execute", "threshold", fetch_method, incident, query=query_dsl, indices=indices)
    _log().debug(
        "Threshold raw_logs: executing query "
        f"source_id={incident.get('source_id')!r}, indices={indices}"
    )
    try:
        response = execute_raw_query(
            es, query_dsl, index=indices, size=RAW_LOGS_FETCH_SIZE, page=0
        )
        hits = response.get("hits", {}).get("hits", [])
        for raw_hit in hits:
            raw_source = raw_hit.get("_source", {}) if isinstance(raw_hit, dict) else {}
            event_data = raw_source.get("event", {}) if isinstance(raw_source, dict) else {}
            event_original = (
                event_data.get("original") if isinstance(event_data, dict) else None
            )
            if event_original is not None:
                incident["raw_logs"].append(str(event_original))
        n_raw = len(incident["raw_logs"])
        _raw_logs_info(
            "done",
            "threshold",
            fetch_method,
            incident,
            es_hits=len(hits) if isinstance(hits, list) else 0,
            raw_logs_count=n_raw,
            hits_found=n_raw > 0,
        )
        _log().debug(
            "Threshold raw_logs: done "
            f"source_id={incident.get('source_id')!r}, "
            f"event.original_hits={n_raw}"
        )
    except Exception as ex:
        incident["raw_logs_error"] = str(ex)
        _raw_logs_info("failed", "threshold", fetch_method, incident, error=str(ex))
        _log().debug(
            f"Threshold raw_logs: failed source_id={incident.get('source_id')!r}, error={ex}"
        )


def format_to_iso(date_string):
    """Normalise an ISO date string to ``YYYY-MM-DDThh:mm:ssZ``."""
    if "." in date_string:
        date_string = date_string.split(".")[0]
    if len(date_string) > 19 and not date_string.endswith("Z"):
        date_string = date_string[:-6]
    if not date_string.endswith("Z"):
        date_string = date_string + "Z"
    return date_string


def _is_numeric_timestamp(value: Any) -> bool:
    """Return ``True`` if ``value`` is already an epoch timestamp."""
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str) and value.strip().isdigit():
        return True
    return False


def _format_utc_iso_z(dt: datetime) -> str:
    """Format ``dt`` as RFC3339 UTC with ``Z`` for Elasticsearch ``@timestamp`` range."""
    dt = dt.astimezone(timezone.utc)
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        frac = f".{dt.microsecond:06d}".rstrip("0").rstrip(".")
        return base + frac + "Z"
    return base + "Z"


def _parse_to_utc_datetime(value: Any) -> Optional[datetime]:
    """Parse fetch/window strings or datetimes to timezone-aware UTC.

    Relative phrases (e.g. ``24 hours``, ``3 days``) are anchored to **current UTC**
    so local machine timezone (e.g. IST) does not shift the query window.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if _is_numeric_timestamp(value):
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    s = str(value).strip()
    parsed = dateparser.parse(
        s,
        settings={
            "RELATIVE_BASE": datetime.now(timezone.utc),
            "TIMEZONE": "UTC",
            "PREFER_DATES_FROM": "past",
        },
    )
    if parsed is None:
        parsed = dateparser.parse(s)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def get_time_range(
    last_fetch: Union[str, None] = None,
    time_range_start: Any = None,
    time_range_end: Any = None,
    time_field: Optional[str] = None,
) -> Dict:
    """Build the time range filter dict for the ``range`` query clause."""
    time_range_start = time_range_start if time_range_start is not None else FETCH_TIME
    time_field = time_field if time_field is not None else TIME_FIELD

    range_dict: Dict[str, Any] = {}
    if not last_fetch and time_range_start:
        if _is_numeric_timestamp(time_range_start):
            start_time = int(time_range_start)
        else:
            start_dt = _parse_to_utc_datetime(time_range_start)
            if start_dt is None:
                _log().debug(f"Could not parse time_range_start={time_range_start!r}")
                start_time = None
            elif TIME_METHOD == "Timestamp-Seconds":
                start_time = int(start_dt.timestamp())
            elif TIME_METHOD == "Timestamp-Milliseconds":
                start_time = int(start_dt.timestamp() * 1000)
            elif TIME_METHOD == "Simple-Date":
                start_time = _format_utc_iso_z(start_dt)
            else:
                start_time = convert_date_to_timestamp(
                    start_dt.replace(tzinfo=None)
                )
    else:
        start_time = last_fetch

    _log().debug(f"Time range start time: {start_time}")
    if start_time:
        range_dict["gt"] = start_time

    if time_range_end:
        if _is_numeric_timestamp(time_range_end):
            end_time = int(time_range_end)
        else:
            end_dt = _parse_to_utc_datetime(time_range_end)
            if end_dt is None:
                _log().debug(f"Could not parse time_range_end={time_range_end!r}")
                end_time = None
            elif TIME_METHOD == "Timestamp-Seconds":
                end_time = int(end_dt.timestamp())
            elif TIME_METHOD == "Timestamp-Milliseconds":
                end_time = int(end_dt.timestamp() * 1000)
            elif TIME_METHOD == "Simple-Date":
                end_time = _format_utc_iso_z(end_dt)
            else:
                end_time = convert_date_to_timestamp(
                    end_dt.replace(tzinfo=None)
                )
        if end_time is not None:
            range_dict["lt"] = end_time

    if TIME_METHOD == "Simple-Date":
        # ISO-8601 ``Z`` is self-describing; legacy format is only for naive strings (e.g. cursors).
        needs_legacy_format = False
        for _key in ("gt", "lt"):
            _v = range_dict.get(_key)
            if isinstance(_v, str) and not _v.endswith("Z"):
                needs_legacy_format = True
                break
        if needs_legacy_format:
            range_dict["format"] = ES_DEFAULT_DATETIME_FORMAT

    if isinstance(time_range_start, str) and (
        utc_offset := re.search(r"([+-]\d{2}:\d{2})$", time_range_start)
    ):
        range_dict["time_zone"] = utc_offset.group(1)

    _log().debug(f"Time range dictionary created: {range_dict}")
    return {"range": {time_field: range_dict}}


def query_string_to_dict(raw_query) -> Dict:
    """Parse a query DSL string/bytearray to a dict body."""
    try:
        if not isinstance(raw_query, dict):
            raw_query = json.loads(raw_query)
        if raw_query.get("query"):
            _log().debug("Query provided already has a query field. Sending as is.")
            body = raw_query
        else:
            body = {"query": raw_query}
    except (ValueError, TypeError) as e:
        body = {"query": raw_query}
        _log().info(f"unable to convert raw query to dictionary, use it as a string\n{e}")
    return body


def execute_raw_query(es, raw_query, index=None, size=None, page=None):
    """Run a raw query DSL body against the configured fetch index."""
    body = _normalize_dsl_query_string_operators(query_string_to_dict(raw_query))
    requested_index = index or FETCH_INDEX

    if isinstance(size, int):
        body["size"] = size
    if isinstance(page, int):
        body["from"] = page

    search = Search(using=es, index=requested_index).update_from_dict(body)

    if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8, OPEN_SEARCH):
        response = search.execute().to_dict()
    else:
        response = es.search(index=search._index, body=search.to_dict(), **search._params)

    _log().debug(f"Raw query response: {response}")
    return response


@task(log_prints=True)
def fetch_incidents(proxies):
    """Implements ``fetch-incidents`` using the runtime state port."""
    last_run = _state().get_last_run()
    last_fetch = last_run.get("time") or FETCH_TIME
    _log().info(
        "Fetch start parameters: "
        f"last_fetch={last_fetch!r}, fetch_query={FETCH_QUERY!r}, "
        f"fetch_index={FETCH_INDEX!r}, time_field={TIME_FIELD!r}, fetch_size={FETCH_SIZE}, "
        f"raw_logs_fetch_size={RAW_LOGS_FETCH_SIZE}"
    )

    es = elasticsearch_builder(proxies)
    time_range_dict = get_time_range(time_range_start=last_fetch)
    time_bounds = (
        time_range_dict.get("range", {}).get(TIME_FIELD, {})
        if isinstance(time_range_dict, dict)
        else {}
    )
    _log().info(
        "Fetch query time bounds: "
        f"start={time_bounds.get('gt')!r}, end={time_bounds.get('lt')!r}"
    )
    time_range_label = _format_time_range_label(time_bounds)

    if RAW_QUERY:
        _log().info(f"Fetch raw_query mode enabled. index={FETCH_INDEX!r}")
        _log().debug(f"Fetch raw_query payload: {RAW_QUERY}")
        query_display = _format_query_for_progress(RAW_QUERY)
        _fetch_progress_print(
            "alerts",
            f"fetching index={FETCH_INDEX!r} size={FETCH_SIZE}\n"
            f"  time_range: {time_range_label}\n"
            f"  query: {query_display}",
        )
        response = execute_raw_query(es, RAW_QUERY)
    else:
        fetch_query = _normalize_query_string_operators(FETCH_QUERY)
        query = QueryString(query="(" + fetch_query + ") AND " + TIME_FIELD + ":*")
        search = Search(using=es, index=FETCH_INDEX).filter(time_range_dict)
        search = search.sort({TIME_FIELD: {"order": "asc"}})[0:FETCH_SIZE].query(query)
        _fetch_progress_print(
            "alerts",
            f"fetching index={FETCH_INDEX!r} size={FETCH_SIZE}\n"
            f"  time_range: {time_range_label}\n"
            f"  query: ({fetch_query}) AND {TIME_FIELD}:*",
        )
        _log().info(
            "Fetch DSL execution: "
            f"index={FETCH_INDEX!r}, size={FETCH_SIZE}, sort_field={TIME_FIELD!r}, sort_order='asc'"
        )
        _log().debug(f"Fetch DSL payload: {search.to_dict()}")

        if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8, OPEN_SEARCH):
            response = search.execute().to_dict()
        else:
            response = es.search(index=search._index, body=search.to_dict(), **search._params)

    _log().debug(f"Fetch incidents response: {response}")
    _, total_results = get_total_results(response)
    _fetch_progress_print(
        "alerts",
        f"fetched {total_results} alert hit(s); converting to incidents",
    )

    incidents: List = []
    if total_results > 0:
        if "Timestamp" in TIME_METHOD:
            incidents, last_fetch = results_to_incidents_timestamp(response, last_fetch, es)
            _state().set_last_run({"time": last_fetch})
        else:
            incidents, last_fetch = results_to_incidents_datetime(
                response, last_fetch or FETCH_TIME, es
            )
            _state().set_last_run({"time": str(last_fetch)})

        _log().info(f"Extracted {len(incidents)} incidents.")
        _fetch_progress_print(
            "alerts",
            f"extracted {len(incidents)} incident(s); raw_log enrichment complete",
        )
        for inc in incidents:
            insert_incident_row_in_supabase(inc)
    else:
        _fetch_progress_print("alerts", "no alert hits in selected time range")
    _output().emit_incidents(incidents)


def parse_subtree(my_map):
    """Recursively walk an Elasticsearch mapping subtree and emit field types."""
    res = {}
    for k in my_map:
        if "properties" in my_map[k]:
            res[k] = parse_subtree(my_map[k]["properties"])
        else:
            res[k] = "type: " + my_map[k].get("type", "")
    return res


def update_elastic_mapping(res_json, elastic_mapping, key):
    """Helper to populate ``elastic_mapping`` for one index ``key``."""
    my_map = res_json[key]["mappings"]["properties"]
    elastic_mapping[key] = {"_id": "doc_id", "_index": key}
    elastic_mapping[key]["_source"] = parse_subtree(my_map)


def get_mapping_fields_command():
    """Implements ``get-mapping-fields`` over the configured ``FETCH_INDEX``."""
    indexes = FETCH_INDEX.split(",")
    elastic_mapping: Dict[str, Any] = {}
    for index in indexes:
        if index == "":
            res = requests.get(
                SERVER + "/_mapping", auth=(USERNAME, PASSWORD), verify=INSECURE
            )
        else:
            res = requests.get(
                SERVER + "/" + index + "/_mapping",
                auth=(USERNAME, PASSWORD),
                verify=INSECURE,
            )
        res_json = res.json()

        if index in ("*", "_all", ""):
            for key in res_json:
                if (
                    "mappings" in res_json[key]
                    and "properties" in res_json[key]["mappings"]
                ):
                    update_elastic_mapping(res_json, elastic_mapping, key)
        elif index.endswith("*"):
            prefix_index = re.compile(index.rstrip("*"))
            for key in res_json:
                if prefix_index.match(key):
                    update_elastic_mapping(res_json, elastic_mapping, key)
        else:
            update_elastic_mapping(res_json, elastic_mapping, index)

    return elastic_mapping


def build_eql_body(query, fields, size, tiebreaker_field, timestamp_field, event_category_field, filter):
    """Assemble the request body for an EQL search."""
    body = {}
    if query is not None:
        body["query"] = query
    if event_category_field is not None:
        body["event_category_field"] = event_category_field
    if fields is not None:
        body["fields"] = fields
    if filter is not None:
        body["filter"] = filter
    if size is not None:
        body["size"] = size
    if tiebreaker_field is not None:
        body["tiebreaker_field"] = tiebreaker_field
    if timestamp_field is not None:
        body["timestamp_field"] = timestamp_field
    return body


def search_eql_command(args, proxies):
    """Implements ``es-eql-search``."""
    index = args.get("index")
    query = args.get("query")
    fields = args.get("fields")
    size = int(args.get("size", "10"))
    timestamp_field = args.get("timestamp_field")
    event_category_field = args.get("event_category_field")
    sort_tiebreaker = args.get("sort_tiebreaker")
    query_filter = args.get("filter")

    es = elasticsearch_builder(proxies)
    body = build_eql_body(
        query=query,
        fields=fields,
        size=size,
        tiebreaker_field=sort_tiebreaker,
        timestamp_field=timestamp_field,
        event_category_field=event_category_field,
        filter=query_filter,
    )

    _log().debug(f"EQL search body: {body}")
    response = es.eql.search(index=index, body=body)

    total_dict, _ = get_total_results(response)
    search_context, meta_headers, hit_tables, hit_headers = results_to_context(
        index, query, 0, size, total_dict, response, event=True,
    )
    search_human_readable = tableToMarkdown(
        "Search Metadata:", search_context, meta_headers, removeNull=True
    )
    hits_human_readable = tableToMarkdown(
        "Hits:", hit_tables, hit_headers, removeNull=True
    )
    total_human_readable = search_human_readable + "\n" + hits_human_readable

    return CommandResults(
        readable_output=total_human_readable,
        outputs_prefix="Elasticsearch.Search",
        outputs=search_context,
    )


def search_esql_command(args, proxies):
    """Implements ``es-esql-search`` (Elasticsearch 8.11+)."""
    query = args.get("query")
    limit = args.get("limit")

    es = elasticsearch_builder(proxies)

    if limit:
        query = {"query": query + f"| LIMIT {limit}"}
    else:
        query = {"query": query}

    if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V8, ELASTICSEARCH_V9):
        compatible_with = 8 if ELASTIC_SEARCH_CLIENT == ELASTICSEARCH_V8 else 9
        headers = {
            "Content-Type": f"application/vnd.elasticsearch+json; "
            f"compatible-with={compatible_with}",
            "Accept": f"application/vnd.elasticsearch+json; "
            f"compatible-with={compatible_with}",
        }
    else:
        _output().emit_error("ES|QL Search is only supported in Elasticsearch 8.11 and above.")
        return None

    _log().debug(f"ES|QL search body: {query}")
    res = es.perform_request(
        method="POST", path="/_query?format=json", headers=headers, body=query,
    )

    human_output_columns = [col["name"] for col in res["columns"]]
    human_output_rows = res["values"]
    human_output = []

    for row in human_output_rows:
        row_dict = {}
        for i, column in enumerate(human_output_columns):
            row_dict[column] = row[i]
        human_output.append(row_dict)

    search_human_readable = tableToMarkdown(
        "Search query:",
        [{"Query": query.get("query"), "Total": str(len(human_output_rows))}],
        removeNull=True,
    )
    hits_human_readable = tableToMarkdown("Results:", human_output, removeNull=True)
    total_human_readable = search_human_readable + "\n" + hits_human_readable

    return CommandResults(
        readable_output=total_human_readable,
        outputs_prefix="Elasticsearch.ESQLSearch",
        outputs=human_output,
        raw_response=getattr(res, "body", res),
    )


def index_document(args, proxies):
    """Index a single document into a target index."""
    index = args.get("index_name")
    doc = args.get("document")
    doc_id = args.get("id", "")
    es = elasticsearch_builder(proxies)

    _log().debug(f"Indexing document in index {index} with ID {doc_id}")
    if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8):
        if doc_id:
            response = es.index(index=index, id=doc_id, document=doc)
        else:
            response = es.index(index=index, document=doc)
    else:
        if doc_id:
            response = es.index(index=index, id=doc_id, body=doc)
        else:
            response = es.index(index=index, body=doc)

    _log().debug(f"Index document response: {response}")
    return response


def index_document_command(args, proxies):
    """Implements ``es-index``."""
    resp = index_document(args, proxies)
    index_context = {
        "id": resp.get("_id", ""),
        "index": resp.get("_index", ""),
        "version": resp.get("_version", ""),
        "result": resp.get("result", ""),
    }
    human_readable = {
        "ID": index_context.get("id"),
        "Index name": index_context.get("index"),
        "Version": index_context.get("version"),
        "Result": index_context.get("result"),
    }
    headers = [str(k) for k in human_readable]
    readable_output = tableToMarkdown(
        name="Indexed document", t=human_readable, removeNull=True, headers=headers,
    )

    if ELASTIC_SEARCH_CLIENT in (ELASTICSEARCH_V9, ELASTICSEARCH_V8):
        resp = getattr(resp, "body", resp)

    return CommandResults(
        readable_output=readable_output,
        outputs_prefix="Elasticsearch.Index",
        outputs=index_context,
        raw_response=resp,
        outputs_key_field="id",
    )


def get_indices_statistics(client):
    """Return raw statistics for every index in the cluster."""
    stats = client.indices.stats()
    return stats.get("indices")


def get_indices_statistics_command(args, proxies):
    """Implements ``es-get-indices-statistics``."""
    limit = arg_to_number(args.get("limit", 50))
    all_results = argToBoolean(args.get("all_results", False))
    indices: List[Dict[str, Any]] = []
    es = elasticsearch_builder(proxies)

    _log().debug("Retrieving indices statistics")
    raw_indices_data = get_indices_statistics(es)
    for index, index_data in raw_indices_data.items():
        index_stats = {
            "Name": index,
            "Status": index_data.get("status", ""),
            "Health": index_data.get("health", ""),
            "UUID": index_data.get("uuid", ""),
            "Documents Count": index_data.get("total", {}).get("docs", {}).get("count", ""),
            "Documents Deleted": index_data.get("total", {}).get("docs", {}).get("deleted", ""),
        }
        indices.append(index_stats)

    if not all_results and limit is not None:
        indices = indices[:limit]

    readable_output = tableToMarkdown(
        name="Indices Statistics:",
        t=indices,
        removeNull=True,
        headers=[str(k) for k in indices[0]] if indices else [],
    )

    return CommandResults(
        readable_output=readable_output,
        outputs_prefix="Elasticsearch.IndexStatistics",
        outputs=indices,
        outputs_key_field="UUID",
        raw_response=raw_indices_data,
    )


@task(log_prints=True)
def get_supabase_client() -> SupabaseClient:
    if not SUPABASE_AVAILABLE or create_client is None:
        raise RuntimeError("Install supabase: pip install supabase")
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


def _local_elastic_params(command: str) -> Dict[str, Any]:
    """Static integration params until Supabase configuration is merged in."""
    return {
        "url": "https://embark-group-f2a75d.es.asia-south1.gcp.elastic.cloud:443",
        "client_type": ELASTICSEARCH_V8,
        "auth_type": API_KEY_AUTH,
        "credentials": {"identifier": "", "password": ""},
        "api_key_auth_credentials": {"identifier": "5fGUwJ4BKNdGrSgn4vdI", "password": "9lgzlcVDUIIzRFr5wPiTHw"},
        "insecure": True,
        "proxy": False,
        "fetch_time_field": "@timestamp",
        "fetch_index": ".alerts-security.alerts-default",
        "fetch_query": "*",
        "raw_query": "",
        "fetch_time": "24 hour"
        ,
        "fetch_size": 20,
        "raw_logs_fetch_size": 5,
        "time_method": "Simple-Date",
        "timeout": 60,
        "map_labels": True,
        "isFetch": command == "fetch-incidents",
    }


@task(log_prints=True)
def get_supabase_params(integration_id: int, command: str) -> Dict[str, Any]:
    """Verify instance exists in Supabase; return local static params for now."""
    supabase = get_supabase_client()
    r = (
        supabase.table("integration_instances")
        .select("configuration")
        .eq("id", integration_id)
        .limit(1)
        .execute()
    )
    if not r.data:
        raise ValueError(f"No integration instance with id={integration_id}")
    cfg = r.data[0].get("configuration")
    if cfg is not None and not isinstance(cfg, dict):
        raise ValueError(f"Invalid configuration type for id={integration_id}")
    return _local_elastic_params(command)


@task(log_prints=True)
def get_last_run_from_supabase(integration_id: int) -> Dict[str, Any]:
    supabase = get_supabase_client()
    r = (
        supabase.table("integration_instances")
        .select("last_run")
        .eq("id", integration_id)
        .limit(1)
        .execute()
    )
    if not r.data:
        return {}
    last_run = r.data[0].get("last_run")
    return last_run if isinstance(last_run, dict) else {}


@task(log_prints=True)
def update_last_run_in_supabase(integration_id: int, last_run: Dict[str, Any]) -> None:
    supabase = get_supabase_client()
    supabase.table("integration_instances").update({"last_run": last_run}).eq(
        "id", integration_id
    ).execute()


def _incident_row_for_supabase(incident: Dict[str, Any]) -> Dict[str, Any]:
    """Build one ``dev_tickets`` row from a fetched incident.

    Drops ``event_count``, ``rule_type``, and ``query``. Encodes ``raw_logs`` as
    JSON text when it is a list (same pattern as Securonix ``raw_logs``).
    """
    row: Dict[str, Any] = {}
    for key, val in incident.items():
        if key in SUPABASE_INCIDENT_INSERT_OMIT_KEYS:
            continue
        if key == "raw_logs":
            if isinstance(val, list):
                row[key] = json.dumps(val, default=str)
            elif val is None:
                row[key] = json.dumps([], default=str)
            else:
                row[key] = val
            continue
        row[key] = val
    return row


@task(log_prints=True)
def insert_incident_row_in_supabase(incident: Dict[str, Any]) -> None:
    """Insert a single fetched incident into Supabase (``dev_tickets``).

    Mirrors ``elastic/drx-securonix.insert_incident_row_in_supabase``; payload
    omits ``event_count``, ``rule_type``, and ``query``.
    """
    if not SUPABASE_AVAILABLE or create_client is None:
        _log().warning("Supabase client unavailable; skipping incident insert.")
        return
    row = _incident_row_for_supabase(incident)
    try:
        supabase = get_supabase_client()
        response = (
            supabase.table(SUPABASE_DEV_TICKETS_TABLE).insert(row).execute()
        )
        if response.data:
            _log().info(
                f"Supabase incident insert ok source_id={row.get('source_id')!r}"
            )
        else:
            _log().warning(
                "Supabase incident insert returned no data "
                f"source_id={row.get('source_id')!r}"
            )
    except Exception as exc:  # noqa: BLE001
        _log().warning(
            f"Supabase incident insert failed source_id={row.get('source_id')!r}: {exc}"
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


@flow(log_prints=True)
def main(integration_id: int = None, command: str = None) -> RuntimeContext:
    """Run the integration using Supabase-backed configuration only."""
    if integration_id is None:
        raise ValueError(
            "Integration ID is required. Usage: main(integration_id=1, command='fetch-incidents')"
        )

    resolved_command = command or "test-module"
    payload = {
        "command": resolved_command,
        "params": get_supabase_params(integration_id, resolved_command),
        "args": {},
        "state": {
            "last_run": get_last_run_from_supabase(integration_id),
            "integration_context": {},
        },
        "log_level": "INFO",
    }

    runtime_ctx = RuntimeContext.from_payload(payload)
    init(runtime_ctx)

    proxies = handle_proxy(params=runtime_ctx.params) or None
    args = runtime_ctx.args
    command = runtime_ctx.command
    log = runtime_ctx.logger

    try:
        log.info(f"command is {command}")
        if command == "test-module":
            runtime_ctx.output.emit_results(test_func(proxies))
        elif command == "fetch-incidents":
            fetch_incidents(proxies)
        elif command in ("search", "es-search"):
            search_command(args, proxies)
        elif command == "get-mapping-fields":
            runtime_ctx.output.emit_results(get_mapping_fields_command())
        elif command == "es-eql-search":
            runtime_ctx.output.emit_results(search_eql_command(args, proxies))
        elif command == "es-esql-search":
            runtime_ctx.output.emit_results(search_esql_command(args, proxies))
        elif command == "es-index":
            runtime_ctx.output.emit_results(index_document_command(args, proxies))
        elif command == "es-integration-health-check":
            runtime_ctx.output.emit_results(integration_health_check(proxies))
        elif command == "es-get-indices-statistics":
            runtime_ctx.output.emit_results(get_indices_statistics_command(args, proxies))
        else:
            runtime_ctx.output.emit_error(
                f"Unknown command: {command!r}", raise_after=False,
            )

    except IntegrationError:
        # Already captured on the output port; re-raise so callers can handle it.
        raise
    except Exception as e:  # noqa: BLE001
        message = str(e)
        if "The client noticed that the server is not a supported distribution of Elasticsearch" in message:
            runtime_ctx.output.emit_error(
                f"Failed executing {command}. Seems that the client does not support "
                f"the server's distribution, Please try using the Open Search client "
                f"in the instance configuration.\nError message: {e!s}",
                raise_after=False,
            )
        elif "failed to parse date field" in message:
            runtime_ctx.output.emit_error(
                f"Failed to execute the {command} command. Make sure the "
                f"`Time field type` is correctly set.",
                raise_after=False,
            )
        else:
            runtime_ctx.output.emit_error(
                f"Failed executing {command}.\nError message: {e}",
                raise_after=False,
            )
    finally:
        try:
            update_last_run_in_supabase(
                integration_id, runtime_ctx.state.get_last_run() or {}
            )
        except Exception as supabase_error:  # noqa: BLE001
            runtime_ctx.logger.warning(
                f"Supabase last_run update failed (id={integration_id}): {supabase_error}"
            )

    return runtime_ctx


if __name__ in ["__main__", "builtin", "builtins"]:
    try:
        integration_id = 1  # Change this to your integration ID
        command = "fetch-incidents"  # Change to "test-module" or other supported command

        ctx = main(integration_id=integration_id, command=command)
        print(json.dumps(ctx.snapshot(), default=str, indent=2))
    except Exception as e:
        print(f"Script execution failed: {e}")
        traceback.print_exc()
