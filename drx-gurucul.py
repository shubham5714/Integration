"""DRX Gurucul GRA integration for non-Demisto execution.

``main(integration_id, command, args=...)`` loads config and state from Supabase,
runs the command, and persists ``last_run``. Flow results persist to a saved
Prefect ``S3Bucket`` block (``s3-bucket/<name>``). For ``health-check``, pass a
single check via ``args`` / ``argue``. I/O uses embedded ``RuntimeContext`` and
CommonServerPython helpers inlined for standalone Python.
"""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from dataclasses import dataclass
import dataclasses as _dc
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, cast

import dateparser
import requests
import urllib3
from prefect import flow, task
from prefect.blocks.system import Secret

try:
    from supabase import Client as SupabaseClient, create_client  # type: ignore[import-not-found]

    SUPABASE_AVAILABLE = True
except ImportError:  # pragma: no cover
    SupabaseClient = Any  # type: ignore[assignment,misc]
    create_client = None  # type: ignore[assignment]
    SUPABASE_AVAILABLE = False


# ---------------------------------------------------------------------------
# Embedded: runtime (mirrors ``elastic/drx-elasticsearch.py``)
# ---------------------------------------------------------------------------


class IntegrationError(Exception):
    """Raised when the integration encounters a fatal error."""


class Logger:
    """Thin logger wrapper that mirrors ``demisto.debug/info/error`` semantics."""

    def __init__(self, name: str = "drx-gurucul-gra", level: int = logging.INFO) -> None:
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

    def warning(self, msg: Any) -> None:
        self._logger.warning(str(msg))

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
# Embedded: CommonServerPython helpers used by this integration
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

    def to_dict(self) -> dict:
        return {
            "outputs_prefix": self.outputs_prefix,
            "outputs_key_field": self.outputs_key_field,
            "outputs": self.outputs,
            "raw_response": self.raw_response,
            "readable_output": self.readable_output,
        }


def urljoin(url: str, suffix: str = "") -> str:
    if url[-1:] != "/":
        url = url + "/"
    if suffix.startswith("/"):
        suffix = suffix[1:]
    return url + suffix


def timestamp_to_datestring(
    timestamp: Any,
    date_format: str = "%Y-%m-%dT%H:%M:%S.000Z",
    is_utc: bool = False,
) -> str:
    use_utc_time = is_utc or date_format.endswith("Z")
    if use_utc_time:
        return datetime.fromtimestamp(int(timestamp) / 1000.0, tz=timezone.utc).strftime(
            date_format
        )
    return datetime.fromtimestamp(int(timestamp) / 1000.0).strftime(date_format)


class BaseClient:
    """Slim HTTP client (CommonServerPython ``BaseClient`` subset)."""

    REQUESTS_TIMEOUT = 60

    def __init__(
        self,
        base_url: str,
        verify: bool = True,
        proxy: bool = False,
        ok_codes: tuple = (),
        headers: Optional[dict] = None,
        auth: Any = None,
        timeout: float = REQUESTS_TIMEOUT,
    ) -> None:
        self._base_url = base_url
        self._verify = verify
        self._ok_codes = ok_codes
        self._headers = headers or {}
        self._auth = auth
        self._session = requests.Session()
        self.timeout = float(timeout)

        if not proxy:
            for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
                os.environ.pop(key, None)

    def _is_status_code_valid(self, response: requests.Response, ok_codes: Any = None) -> bool:
        status_codes = ok_codes if ok_codes is not None else self._ok_codes
        if status_codes:
            return response.status_code in status_codes
        return response.ok

    def _http_request(
        self,
        method: str,
        url_suffix: str = "",
        full_url: Optional[str] = None,
        headers: Optional[dict] = None,
        auth: Any = None,
        json_data: Any = None,
        params: Optional[dict] = None,
        data: Any = None,
        files: Any = None,
        timeout: Optional[float] = None,
        resp_type: str = "json",
        ok_codes: Any = None,
        **kwargs: Any,
    ) -> Any:
        address = full_url if full_url else urljoin(self._base_url, url_suffix)
        headers = headers if headers is not None else self._headers
        auth = auth if auth is not None else self._auth
        request_timeout = self.timeout if timeout is None else timeout

        try:
            res = self._session.request(
                method,
                address,
                verify=self._verify,
                params=params,
                data=data,
                json=json_data,
                files=files,
                headers=headers,
                auth=auth,
                timeout=request_timeout,
                **kwargs,
            )
        except requests.exceptions.ConnectTimeout as exception:
            raise DemistoException(
                "Connection Timeout Error - potential reasons might be that the Server URL "
                "parameter is incorrect or that the Server is not accessible from your host."
            ) from exception
        except requests.exceptions.SSLError as exception:
            raise DemistoException(
                "SSL Certificate Verification Failed - try selecting 'Trust any certificate' "
                "in the instance configuration."
            ) from exception
        except requests.exceptions.ProxyError as exception:
            raise DemistoException(
                "Proxy Error - if the 'Use system proxy' checkbox is selected, try clearing it."
            ) from exception
        except requests.exceptions.ConnectionError as exception:
            raise DemistoException(
                f"Failed to establish a new connection.\nError: {exception}"
            ) from exception
        except requests.exceptions.RequestException as exception:
            raise DemistoException(f"Request failed: {exception}") from exception

        if not self._is_status_code_valid(res, ok_codes):
            raise DemistoException(f"Error in API call [{res.status_code}] - {res.text}")

        if resp_type == "json":
            try:
                return res.json()
            except ValueError as exception:
                raise DemistoException(
                    f"Failed to parse response as JSON. Response: {res.text}"
                ) from exception
        if resp_type == "text":
            return res.text
        if resp_type == "content":
            return res.content
        if resp_type == "response":
            return res
        raise ValueError(f"Invalid resp_type: {resp_type}")


# ---------------------------------------------------------------------------
# Constants / module runtime
# ---------------------------------------------------------------------------

urllib3.disable_warnings()

MAX_INCIDENTS_TO_FETCH = 25
API_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
SUPABASE_URL = "https://zhhsijigoupqroztdrdy.supabase.co"

# Sync load — module-level ``await`` is invalid when Prefect imports this as a script.
try:
    supabase_api_key = Secret.load("supabase-api-key")
    SUPABASE_ANON_KEY = supabase_api_key.get()
except Exception:
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")
    if not SUPABASE_ANON_KEY:
        raise RuntimeError(
            "Prefect Secret 'supabase-api-key' not found. "
            "Set env SUPABASE_ANON_KEY for local runs."
        )

SUPABASE_DEV_TICKETS_TABLE = "dev_tickets"

_runtime: Optional[RuntimeContext] = None


def _runtime_or_raise() -> RuntimeContext:
    if _runtime is None:
        raise IntegrationError(
            "Runtime not initialized. Invoke ``main(integration_id, command)`` before using "
            "module-level helpers."
        )
    return _runtime


def _log() -> Logger:
    return _runtime_or_raise().logger


def _state() -> StatePort:
    return _runtime_or_raise().state


def _output() -> OutputPort:
    return _runtime_or_raise().output


def init(runtime: RuntimeContext) -> None:
    """Bind a runtime context to this module."""
    global _runtime
    _runtime = runtime


def return_results(results: Any) -> None:
    if results is None:
        return
    if isinstance(results, CommandResults):
        _output().emit_results(results.to_dict())
    else:
        _output().emit_results(results)


def return_error(message: str, error: Any = "", outputs: Any = None) -> None:
    if error:
        _log().error(str(error))
    _output().emit_error(str(message), raise_after=True)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class Client(BaseClient):
    def fetch_command_result(self, url_suffix, params, post_url):
        incidents: list = []
        try:
            if post_url is None:
                method = "GET"
            else:
                method = "POST"
                params = None
            r = self._http_request(method=method, url_suffix=url_suffix, data=post_url, params=params)
            incidents = r if isinstance(r, list) else [r]
        except Exception:
            _log().error("Unable to fetch command result" + traceback.format_exc())
        return incidents

    def validate_api_key(self):
        self._http_request(method="GET", url_suffix="/validate", params={})
        return "ok"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def arg_to_int(arg: Any, arg_name: str, required: bool = False) -> int | None:
    if arg is None:
        if required is True:
            raise ValueError(f'Missing "{arg_name}"')
        return None
    if isinstance(arg, str):
        if arg.isdigit():
            return int(arg)
        raise ValueError(f'Invalid number: "{arg_name}"="{arg}"')
    if isinstance(arg, int):
        return arg
    raise ValueError(f'Invalid number: "{arg_name}"')


def arg_to_timestamp(arg: Any, arg_name: str, required: bool = False) -> int | None:
    if arg is None:
        if required is True:
            raise ValueError(f'Missing "{arg_name}"')
        return None

    if isinstance(arg, str) and arg.isdigit():
        return int(arg)
    if isinstance(arg, str):
        date = dateparser.parse(arg, settings={"TIMEZONE": "UTC"})
        if date is None:
            raise ValueError(f"Invalid date: {arg_name}")
        return int(date.timestamp())
    if isinstance(arg, int | float):
        return int(arg)
    raise ValueError(f'Invalid date: "{arg_name}"')


# ---------------------------------------------------------------------------
# Command functions
# ---------------------------------------------------------------------------


def fetch_record_command(client: Client, url_suffix, prefix, key, params, post_url=None):
    incidents: list = []
    r = client.fetch_command_result(url_suffix, params, post_url)
    incidents.extend(r)
    results = CommandResults(outputs_prefix=prefix, outputs_key_field=key, outputs=incidents)
    return results


def fetch_records(client: Client, url_suffix, prefix, key, params):
    results = fetch_record_command(client, url_suffix, prefix, key, params)
    return_results(results)


def fetch_post_records(client: Client, url_suffix, prefix, key, params, post_url):
    results = fetch_record_command(client, url_suffix, prefix, key, params, post_url)
    return_results(results)


def _format_api_datetime(dt: datetime) -> str:
    """Format datetime like the n8n helper: ``YYYY-MM-DD HH:MM:SS`` (UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime(API_DATE_FORMAT)


def _coerce_api_datetime(value: Any) -> str | None:
    """Normalize a date/time arg to ``YYYY-MM-DD HH:MM:SS`` UTC, or None if empty."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _format_api_datetime(value)
    if isinstance(value, (int, float)):
        return _format_api_datetime(datetime.fromtimestamp(int(value), tz=timezone.utc))
    text = str(value).strip()
    if not text:
        return None
    # Already GRA-shaped — keep as-is.
    try:
        datetime.strptime(text, API_DATE_FORMAT)
        return text
    except ValueError:
        pass
    parsed = dateparser.parse(text, settings={"TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True})
    if parsed is None:
        raise ValueError(f"Invalid datetime: {value!r}")
    return _format_api_datetime(parsed)


def resolve_search_time_window(
    *,
    from_date: Any = None,
    to_date: Any = None,
    period: Any = None,
    default_period: str | None = None,
) -> tuple[str | None, str | None]:
    """Resolve ``eventFromDate``/``eventToDate`` for searchBigDataEvents.

    Priority:
      1. Explicit from_date / to_date (aliases: fromDate, startDate, eventFromDate, …)
      2. Relative ``period`` (e.g. ``1 hour``) ending at now
      3. ``default_period`` when neither explicit dates nor period are set
    """
    from_s = _coerce_api_datetime(from_date)
    to_s = _coerce_api_datetime(to_date)
    if from_s or to_s:
        if from_s and not to_s:
            to_s = _format_api_datetime(datetime.now(timezone.utc))
        return from_s, to_s

    rel = (str(period).strip() if period not in (None, "") else None) or default_period
    if not rel:
        return None, None

    now = datetime.now(timezone.utc)
    rel_text = rel if "ago" in rel.lower() else f"{rel} ago"
    start = dateparser.parse(
        rel_text,
        settings={
            "TIMEZONE": "UTC",
            "RETURN_AS_TIMEZONE_AWARE": True,
            "RELATIVE_BASE": now,
        },
    )
    if start is None:
        raise ValueError(f"Invalid period: {rel!r}")
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    else:
        start = start.astimezone(timezone.utc)
    if start > now:
        start = now - (start - now)
    return _format_api_datetime(start), _format_api_datetime(now)


def _parse_alert_detection(detection_timestamp: Any) -> datetime | None:
    """Parse GRA ``detectionTimestamp`` (e.g. ``07/21/2026 18:04:38``) as UTC-aware datetime."""
    if detection_timestamp is None or detection_timestamp == "":
        return None
    return dateparser.parse(
        str(detection_timestamp),
        settings={"TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True},
    )


def _alert_detection_to_occurred_at(detection_timestamp: Any) -> str:
    """Convert GRA ``detectionTimestamp`` to UTC ISO."""
    parsed = _parse_alert_detection(detection_timestamp)
    if parsed is None:
        return timestamp_to_datestring(
            datetime.now(timezone.utc).timestamp() * 1000, is_utc=True
        )
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _alert_detection_to_unix(detection_timestamp: Any) -> int | None:
    """Convert GRA ``detectionTimestamp`` to unix seconds (UTC)."""
    parsed = _parse_alert_detection(detection_timestamp)
    if parsed is None:
        return None
    return int(parsed.timestamp())


def _resolve_severity_thresholds(params: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
    """Read high/medium/low severity score bounds from params."""
    params = params or {}
    high_min = arg_to_int(params.get("severity_high_min", 71), "severity_high_min") or 71
    medium_min = arg_to_int(params.get("severity_medium_min", 31), "severity_medium_min") or 31
    low_min = arg_to_int(params.get("severity_low_min", 0), "severity_low_min") or 0
    high_max = arg_to_int(params.get("severity_high_max", 100), "severity_high_max") or 100
    return {
        "high_min": high_min,
        "medium_min": medium_min,
        "low_min": low_min,
        "high_max": high_max,
        "medium_max": high_min - 1,
        "low_max": medium_min - 1,
    }


def map_severity_label(severity: Any, thresholds: Dict[str, int]) -> str:
    """Map numeric GRA severity to High / Medium / Low."""
    high_min = thresholds["high_min"]
    medium_min = thresholds["medium_min"]

    try:
        score = int(severity)
    except (TypeError, ValueError):
        return str(severity) if severity is not None else "Low"

    if score >= high_min:
        return "High"
    if score >= medium_min:
        return "Medium"
    return "Low"


def _export_defaults_from_params(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Static ticket fields from local params (mirrors Elastic export defaults)."""
    params = params or {}
    return {
        "instance_name": params.get("instance_name", "Gurucul-GRA"),
        "tenant_id": params.get("tenant_id", ""),
        "tenant_name": params.get("tenant_name", ""),
        "type": params.get("type", "gurucul"),
        "alert_source": params.get("alert_source", ""),
    }


def _finalize_incident_for_export(
    incident: Dict[str, Any], export_defaults: Dict[str, Any]
) -> None:
    """Attach ``ai_message`` and param-driven export fields before insert."""
    logs = incident.get("raw_logs")
    first = logs[0] if isinstance(logs, list) and logs else ""
    incident["ai_message"] = {
        "name": incident.get("name"),
        "severity": incident.get("severity"),
        "occurred_at": incident.get("occurred_at"),
        "raw_log": first,
    }
    for key, value in export_defaults.items():
        incident.setdefault(key, value)


def _strip_empty_string_fields(value: Any) -> Any:
    """Drop keys whose value is ``""`` from dicts (recursively); clean lists item-wise."""
    if isinstance(value, dict):
        return {
            k: _strip_empty_string_fields(v)
            for k, v in value.items()
            if v != ""
        }
    if isinstance(value, list):
        return [_strip_empty_string_fields(item) for item in value]
    return value


def _parse_big_data_events(response: Any) -> list:
    if isinstance(response, dict):
        result = response.get("Result") or []
        events = result if isinstance(result, list) else [result]
    elif isinstance(response, list):
        events = response
    else:
        events = []
    return _strip_empty_string_fields(events)


def search_big_data_events(
    client: Client,
    expression: str,
    page: int = 1,
    max_events: int = 100,
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    retries: int = 1,
    retry_delay_sec: float = 2.0,
    log_prefix: str = "[search]",
) -> list:
    """POST ``/v1/searchBigDataEvents`` with a GRA expression.

    Optional ``from_date`` / ``to_date`` (``YYYY-MM-DD HH:MM:SS``) are sent as
    ``eventFromDate`` / ``eventToDate``.
    Retries when the first attempt returns no events (empty Result or request error).
    """
    expression = (expression or "").strip()
    if not expression:
        raise ValueError("expression/query is required")

    headers = dict(client._headers or {})
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    headers["Accept"] = "application/json"
    page = max(1, int(page))
    max_events = max(1, min(int(max_events), 500))
    attempts = max(1, int(retries) + 1)

    form: Dict[str, str] = {
        "expression": expression,
        "page": str(page),
        "max": str(max_events),
    }
    if from_date:
        form["eventFromDate"] = from_date
    if to_date:
        form["eventToDate"] = to_date
    print(
        f"{log_prefix} expression={expression!r} page={page} max={max_events} "
        f"eventFromDate={form.get('eventFromDate')!r} "
        f"eventToDate={form.get('eventToDate')!r}"
    )

    def _once(attempt: int) -> list:
        try:
            response = client._http_request(
                method="POST",
                url_suffix="/v1/searchBigDataEvents",
                data=form,
                headers=headers,
                resp_type="json",
            )
            events = _parse_big_data_events(response)
            if not events:
                print(
                    f"{log_prefix} attempt={attempt} empty Result "
                    f"expression={expression!r} response={response!r}"
                )
            return events
        except Exception:
            err = traceback.format_exc()
            print(f"{log_prefix} attempt={attempt} error:\n{err}")
            _log().error(f"Unable to searchBigDataEvents expression={expression!r}: {err}")
            return []

    events = _once(attempt=1)
    for attempt in range(2, attempts + 1):
        if events:
            break
        print(f"{log_prefix} events=0 — retrying (attempt {attempt}/{attempts})")
        time.sleep(retry_delay_sec)
        events = _once(attempt=attempt)
    if not events:
        print(f"{log_prefix} still 0 events after {attempts} attempt(s) — continuing")
    return events


def fetch_alert_raw_logs(
    client: Client, alert_id: Any, max_events: int = 25
) -> list:
    """Fetch big-data events for an alert via ``search_big_data_events``."""
    return search_big_data_events(
        client,
        expression=f'gra.alertid = "AL-{alert_id}"',
        page=1,
        max_events=max_events,
        retries=1,
        log_prefix=f"[raw_logs] alertId={alert_id}",
    )


def gra_search_command(
    client: Client,
    arguments: Dict[str, Any],
) -> list:
    """Run ``gra-search``: execute a GRA expression and return Result rows.

    Time window args (optional):
      - ``eventFromDate`` / ``eventToDate`` (also ``fromDate`` / ``toDate`` / ``startDate`` / ``endDate``)
      - or relative ``period`` (e.g. ``1 hour``, ``24 hours``)
    """
    expression = arguments.get("query") or arguments.get("expression")
    if not expression:
        raise ValueError("gra-search requires args.query (or args.expression)")
    page = arg_to_int(arg=arguments.get("page"), arg_name="page", required=False) or 1
    max_events = (
        arg_to_int(arg=arguments.get("max"), arg_name="max", required=False) or 100
    )
    from_date, to_date = resolve_search_time_window(
        from_date=arguments.get("eventFromDate")
        or arguments.get("fromDate")
        or arguments.get("startDate"),
        to_date=arguments.get("eventToDate")
        or arguments.get("toDate")
        or arguments.get("endDate"),
        period=arguments.get("period"),
    )
    print(
        f"[gra-search] expression={expression!r} page={page} max={max_events} "
        f"eventFromDate={from_date!r} eventToDate={to_date!r}"
    )
    events = search_big_data_events(
        client,
        expression=str(expression),
        page=page,
        max_events=max_events,
        from_date=from_date,
        to_date=to_date,
        retries=1,
        log_prefix="[gra-search]",
    )
    print(f"[gra-search] result_count={len(events)}")
    return events


def _normalize_health_items(items: Any) -> list[dict[str, str]]:
    """Coerce ``health_check_list`` / ``items`` into ``[{type, value, severity}, ...]``.

    Accepts:
      - ``[{"type": "Firewall", "value": "HOST", "severity": "High"}, ...]``
      - ``["HOST", ...]`` / CSV string (type/severity default empty)
    """
    if items is None:
        return []
    if isinstance(items, str):
        text = items.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            items = parsed
        except json.JSONDecodeError:
            return [
                {"type": "", "value": part.strip(), "severity": ""}
                for part in text.split(",")
                if part.strip()
            ]

    if not isinstance(items, (list, tuple, set)):
        items = [items]

    normalized: list[dict[str, str]] = []
    for entry in items:
        if isinstance(entry, dict):
            value = entry.get("value")
            if value in (None, ""):
                value = entry.get("hostname") or entry.get("name") or entry.get("item")
            if value in (None, ""):
                continue
            item_type = entry.get("type") or entry.get("item_type") or ""
            severity = entry.get("severity") or ""
            normalized.append(
                {
                    "type": str(item_type).strip(),
                    "value": str(value).strip(),
                    "severity": str(severity).strip(),
                }
            )
        else:
            text = str(entry).strip()
            if text:
                normalized.append({"type": "", "value": text, "severity": ""})
    return normalized


def _health_check_ticket_row(
    *,
    item: dict[str, str],
    instance_name: str,
    tenant_id: Any,
    health_check_id: Any = None,
) -> Dict[str, Any]:
    """Build a ``dev_tickets`` row for a missing health-check item.

    ``health_check_id`` is the assessment check id (``assessment_check`` from args).
    """
    value = item.get("value") or ""
    item_type = item.get("type") or "device"
    severity = item.get("severity") or "High"
    now_utc = datetime.now(timezone.utc)
    occurred_at = now_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    created_at = occurred_at
    name = f"Log Stoppage | {value}- {item_type} | {instance_name}"
    message = f"log stoppage observed for this {item_type}: {value}"
    raw_logs = [
        {
            "item": value,
            "type": item_type,
            "instance_name": instance_name,
            "message": message,
        }
    ]
    payload = {
        "name": name,
        "severity": severity,
        "events": raw_logs,
    }

    row: Dict[str, Any] = {
        "name": name,
        "severity": severity,
        "raw_logs": raw_logs,
        "created_at": created_at,
        "occurred_at": occurred_at,
        "instance_name": instance_name,
        "rawJSON": payload,
        "ai_message": payload,
        "tenant_id": tenant_id if tenant_id not in (None, "") else "",
        "log_source": "Health Check",
        "alert_source": "/assets/images/brand-logos/desktop-dark-2.png",
        "source_id": "",
        "type": "health-check",
    }
    if health_check_id not in (None, ""):
        row["health_check_id"] = health_check_id
    return row


def _match_key(value: Any) -> str:
    """Case-insensitive key used to compare expected items vs Result values."""
    return str(value).strip().casefold()


def _values_from_search_result(events: list, match_field: str) -> dict[str, str]:
    """Map normalized match keys → original Result values for ``match_field``."""
    found: dict[str, str] = {}
    for row in events:
        raw: Any = None
        if isinstance(row, dict):
            if match_field in row and row[match_field] not in (None, ""):
                raw = row[match_field]
            elif len(row) == 1:
                only = next(iter(row.values()))
                if only not in (None, ""):
                    raw = only
        elif row not in (None, ""):
            raw = row
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            found[_match_key(text)] = text
    return found


@task(log_prints=True, persist_result=False)
def gra_health_check_command(
    client: Client,
    instance_id: int,
    params: Optional[Dict[str, Any]] = None,
    check_args: Optional[Dict[str, Any]] = None,
) -> dict[str, Any]:
    """Run one health check from ``check_args``: query GRA and flag missing expected items.

    Required ``check_args`` keys: ``query``, ``health_check_list`` (or ``items``).

    Optional: ``match_field``, ``assessment_check``, ``mail_notification_group``,
    ``chat_notification_group``, ``name``, ``page``, ``max``, ``period``, date fields,
    ``item_label``, ``tenant_id`` (from assessment_check; used on ``dev_tickets`` only).
    """
    params = params or {}
    check = dict(check_args or {})
    instance_name = str(params.get("instance_name") or f"instance-{instance_id}")
    assessment_check = check.get("assessment_check")
    check_id = (
        assessment_check
        if assessment_check not in (None, "")
        else check.get("id") or check.get("health_check_id")
    )
    check_name = (
        check.get("name")
        or check.get("check_name")
        or (f"assessment-{assessment_check}" if assessment_check not in (None, "") else None)
        or (f"check-{check_id}" if check_id not in (None, "") else "health-check")
    )
    mail_notification_group = check.get("mail_notification_group")
    chat_notification_group = check.get("chat_notification_group")

    summary: dict[str, Any] = {
        "instance_id": instance_id,
        "assessment_check": assessment_check,
        "mail_notification_group": mail_notification_group,
        "chat_notification_group": chat_notification_group,
        "missing_total": 0,
        "tickets_inserted": 0,
    }

    query = check.get("query") or ""
    match_field = str(check.get("match_field") or "hostname").strip() or "hostname"
    default_item_label = str(check.get("item_label") or "device").strip() or "device"
    tenant_id = check.get("tenant_id")
    expected_items = _normalize_health_items(
        check.get("health_check_list")
        if check.get("health_check_list") not in (None, "")
        else check.get("items")
    )
    expected_values = [item["value"] for item in expected_items]
    page = arg_to_int(arg=check.get("page"), arg_name="page", required=False) or 1
    max_events = arg_to_int(arg=check.get("max"), arg_name="max", required=False) or 100
    from_date, to_date = resolve_search_time_window(
        from_date=check.get("eventFromDate")
        or check.get("fromDate")
        or check.get("from_date")
        or check.get("startDate"),
        to_date=check.get("eventToDate")
        or check.get("toDate")
        or check.get("to_date")
        or check.get("endDate"),
        period=check.get("period"),
        default_period="1 hour",
    )

    if not query:
        print(f"[health-check] skip name={check_name!r} — empty query")
        summary.update(
            {
                "id": check_id,
                "name": check_name,
                "status": "skipped",
                "reason": "empty query",
                "missing": [],
            }
        )
        return summary

    if not expected_items:
        print(f"[health-check] skip name={check_name!r} — empty health_check_list")
        summary.update(
            {
                "id": check_id,
                "name": check_name,
                "status": "skipped",
                "reason": "empty health_check_list",
                "missing": [],
            }
        )
        return summary

    print(
        f"[health-check] start name={check_name!r} id={check_id} "
        f"assessment_check={assessment_check!r} "
        f"match_field={match_field!r} expected={len(expected_items)} "
        f"eventFromDate={from_date!r} eventToDate={to_date!r} query={query!r}"
    )

    events = search_big_data_events(
        client,
        expression=str(query),
        page=page,
        max_events=max_events,
        from_date=from_date,
        to_date=to_date,
        retries=1,
        log_prefix=f"[health-check] name={check_name!r}",
    )
    found_map = _values_from_search_result(events, match_field)
    found_keys = set(found_map.keys())
    found_values = sorted(found_map.values(), key=str.casefold)
    missing_items = [
        item for item in expected_items if _match_key(item["value"]) not in found_keys
    ]
    matched_items = [
        item for item in expected_items if _match_key(item["value"]) in found_keys
    ]
    expected_keys = {_match_key(v) for v in expected_values}
    extra = [value for key, value in found_map.items() if key not in expected_keys]

    print(
        f"[health-check] Result ({len(events)} row(s)) for "
        f"match_field={match_field!r}:\n"
        f"{json.dumps(events, default=str, indent=2)}"
    )
    print(
        f"[health-check] extracted {match_field} values "
        f"({len(found_values)}): {found_values}"
    )
    print(f"[health-check] expected values ({len(expected_values)}): {expected_values}")
    if extra:
        print(
            f"[health-check] in Result but not in health_check_list "
            f"({len(extra)}): {extra}"
        )

    tickets_for_check = 0
    for item in missing_items:
        label = item["type"] or default_item_label
        print(
            f"[health-check] log stoppage observed for this {label}: {item['value']} "
            f"severity={item.get('severity')!r} (check={check_name!r})"
        )
        ticket = _health_check_ticket_row(
            item=item,
            instance_name=instance_name,
            tenant_id=tenant_id,
            health_check_id=check_id,
        )
        print(
            f"[health-check] inserting ticket name={ticket['name']!r} "
            f"severity={ticket['severity']!r}"
        )
        insert_incident_row_in_supabase(ticket)
        tickets_for_check += 1

    print(
        f"[health-check] done name={check_name!r} "
        f"result_count={len(events)} found={len(matched_items)} "
        f"missing={len(missing_items)} tickets={tickets_for_check}"
    )

    summary.update(
        {
            "id": check_id,
            "name": check_name,
            "status": "ok" if not missing_items else "stoppage",
            "match_field": match_field,
            "eventFromDate": from_date,
            "eventToDate": to_date,
            "expected_count": len(expected_items),
            "found_count": len(matched_items),
            "result_count": len(events),
            "result": events,
            "found_values": found_values,
            "missing": missing_items,
            "extra": extra,
            "tickets_inserted": tickets_for_check,
            "missing_total": len(missing_items),
        }
    )

    print(
        f"[health-check] complete instance_id={instance_id} "
        f"assessment_check={assessment_check!r} "
        f"missing_total={summary['missing_total']} "
        f"tickets_inserted={summary['tickets_inserted']}"
    )
    return summary


@task(log_prints=True, persist_result=False)
def fetch_incidents(
    client: Client,
    max_results: int,
    last_run: dict[str, int],
    first_fetch_time: int | None,
    raw_logs_fetch_size: int = 25,
    severity_thresholds: Optional[Dict[str, int]] = None,
    export_defaults: Optional[Dict[str, Any]] = None,
) -> tuple[dict[str, int], list[dict]]:
    """Fetch GRA alerts via ``/alerts/All`` and enrich each with big-data events.

    Lists the date window (API is descending), sorts ascending by
    ``detectionTimestamp``, then fetches ``raw_logs`` only for the oldest
    ``max_results`` alerts. Cursor advances to the latest processed detection
    time when more candidates remain; otherwise to now.
    """
    thresholds = severity_thresholds or _resolve_severity_thresholds()
    export_defaults = export_defaults or _export_defaults_from_params()
    last_fetch = last_run.get("last_fetch", None)
    prev_max_alert_id = last_run.get("maxAlertId")

    now_utc = datetime.now(timezone.utc)
    end_date = _format_api_datetime(now_utc)
    url_access_time = int(now_utc.timestamp())

    if last_fetch is None:
        last_fetch = first_fetch_time
        start_date = _format_api_datetime(
            datetime.fromtimestamp(cast(int, last_fetch), tz=timezone.utc).replace(
                microsecond=0, second=0
            )
        )
        print(f"[fetch] first run — using first_fetch window")
    else:
        # Inclusive resume: same-second alerts are deduped via maxAlertId.
        last_fetch = int(last_fetch)
        start_date = _format_api_datetime(
            datetime.fromtimestamp(cast(int, last_fetch), tz=timezone.utc)
        )
        print(f"[fetch] resume from last_fetch={last_fetch}, maxAlertId={prev_max_alert_id}")

    print(f"[fetch] time window from={start_date!r} to={end_date!r}")
    print(f"[fetch] max_fetch={max_results}, raw_logs_fetch_size={raw_logs_fetch_size}")
    print(
        f"[fetch] severity bands "
        f"High ({thresholds['high_min']}–{thresholds['high_max']}), "
        f"Medium ({thresholds['medium_min']}–{thresholds['medium_max']}), "
        f"Low ({thresholds['low_min']}–{thresholds['low_max']})"
    )

    alerts_url = "/alerts/All"
    incidents: list[dict[str, Any]] = []

    # List all alerts in the window first (API returns descending). Sort ascending,
    # then take the oldest ``max_results`` before any raw_logs calls.
    list_page_size = max(max_results, 100)
    all_alerts: list[dict[str, Any]] = []
    page = 1
    while True:
        list_params = {
            "startDate": start_date,
            "endDate": end_date,
            "page": page,
            "max": list_page_size,
        }
        print(f"[fetch] listing alerts page={page} max={list_page_size}")
        batch = client.fetch_command_result(alerts_url, list_params, None)
        if not batch:
            print(f"[fetch] page={page} returned 0 alerts — stop listing")
            break
        print(f"[fetch] page={page} returned {len(batch)} alert(s)")
        all_alerts.extend(r for r in batch if isinstance(r, dict))
        if len(batch) < list_page_size:
            break
        page += 1

    def _sort_key(rec: dict) -> tuple:
        ts = _alert_detection_to_unix(rec.get("detectionTimestamp"))
        aid = rec.get("alertId") or 0
        return (ts if ts is not None else 0, aid)

    all_alerts.sort(key=_sort_key)  # ascending: oldest → newest
    print(f"[fetch] listed {len(all_alerts)} alert(s) total; sorted ascending by detectionTimestamp")

    candidates: list[dict[str, Any]] = []
    for record in all_alerts:
        alert_id = record.get("alertId")
        if alert_id is None:
            continue
        if prev_max_alert_id is not None and alert_id <= prev_max_alert_id:
            continue
        candidates.append(record)

    to_process = candidates[:max_results]
    more_remaining = len(candidates) > max_results
    print(
        f"[fetch] candidates={len(candidates)} after dedupe; "
        f"processing={len(to_process)} (max_fetch); more_remaining={more_remaining}"
    )

    latest_detection_unix: int | None = None
    temp_max_alert_id = prev_max_alert_id

    for idx, record in enumerate(to_process, start=1):
        alert_id = record.get("alertId")
        if temp_max_alert_id is None or alert_id > temp_max_alert_id:
            temp_max_alert_id = alert_id

        detection_unix = _alert_detection_to_unix(record.get("detectionTimestamp"))
        if detection_unix is not None and (
            latest_detection_unix is None or detection_unix > latest_detection_unix
        ):
            latest_detection_unix = detection_unix

        print(
            f"[raw_logs] ({idx}/{len(to_process)}) alertId={alert_id} "
            f"detectionTimestamp={record.get('detectionTimestamp')!r} "
            f"anomalyName={record.get('anomalyName')!r}"
        )
        raw_logs = fetch_alert_raw_logs(
            client, alert_id, max_events=raw_logs_fetch_size
        )
        print(f"[raw_logs] alertId={alert_id} events={len(raw_logs)}")
        record["incidentType"] = record.get("incidentType") or "GRAAlert"
        raw_payload = dict(record)
        raw_payload["events"] = raw_logs

        incidents.append(
            {
                "name": record.get("anomalyName") or record.get("entity") or str(alert_id),
                "occurred_at": _alert_detection_to_occurred_at(record.get("detectionTimestamp")),
                "severity": map_severity_label(record.get("severity"), thresholds),
                "source_id": str(alert_id),
                "rawJSON": raw_payload,
                "raw_logs": raw_logs,
            }
        )
        _finalize_incident_for_export(incidents[-1], export_defaults)

    # More candidates left after max_fetch → resume from latest processed detection.
    # Otherwise window drained → jump to now.
    if more_remaining and latest_detection_unix is not None:
        next_last_fetch = latest_detection_unix
    else:
        next_last_fetch = url_access_time

    next_run: dict[str, int] = {
        "last_fetch": next_last_fetch,
    }
    if temp_max_alert_id is not None:
        next_run["maxAlertId"] = temp_max_alert_id

    print(
        f"[fetch] done incidents={len(incidents)} next_run={next_run} "
        f"next_last_fetch_utc={_format_api_datetime(datetime.fromtimestamp(next_last_fetch, tz=timezone.utc))}"
    )
    return next_run, incidents


@task(log_prints=True, persist_result=False)
def test_module_command(client: Client) -> str:
    try:
        client.validate_api_key()
    except DemistoException as e:
        if "Forbidden" in str(e):
            return "Authorization Error: make sure API Key is correctly set"
        else:
            raise e
    return "ok"


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

@task(log_prints=True, persist_result=False)
def get_supabase_client() -> SupabaseClient:
    if not SUPABASE_AVAILABLE or create_client is None:
        raise RuntimeError("Install supabase: pip install supabase")
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


# Keys taken from Supabase ``configuration`` JSON; everything else stays local.
SUPABASE_CONFIGURATION_KEYS = (
    "url",
    "apikey",
    "max_fetch",
    "first_fetch",
    "severity_low_min",
    "severity_high_min",
    "severity_medium_min",
    "raw_logs_fetch_size",
)


def _local_gurucul_params(command: str) -> Dict[str, Any]:
    """Local defaults. Supabase overlays fetch/auth keys and tenant/branding fields."""
    return {
        "url": "",
        "apikey": "",
        "insecure": True,
        "proxy": False,
        "first_fetch": "1 days",
        "max_fetch": 1,  # alerts processed per run (oldest first)
        "raw_logs_fetch_size": 2,  # events per alert from searchBigDataEvents
        # Severity score bands → labels High / Medium / Low
        "severity_high_min": 71,
        "severity_high_max": 100,
        "severity_medium_min": 31,
        "severity_low_min": 0,
        # Export / dev_tickets defaults (tenant_id comes from Supabase instance row)
        "instance_name": "",
        "tenant_name": "",
        "type": "default",
        "alert_source": "/assets/images/brand-logos/gurucul-logo.png",
        "isFetch": command == "fetch-incidents",
    }


@task(log_prints=True, persist_result=False)
def get_supabase_params(integration_id: int, command: str) -> Dict[str, Any]:
    """Merge local defaults with selected Supabase configuration + instance fields.

    From ``configuration`` JSON only:
      url, apikey, max_fetch, first_fetch, severity_*, raw_logs_fetch_size

    From instance row:
      tenant_id, instance_name, tenant_name, logo → alert_source
    """
    supabase = get_supabase_client()
    r = (
        supabase.table("integration_instances")
        .select("configuration, tenant_id, instance_name, tenant_name, logo")
        .eq("id", integration_id)
        .limit(1)
        .execute()
    )
    if not r.data:
        raise ValueError(f"No integration instance with id={integration_id}")

    row = r.data[0]
    cfg = row.get("configuration")
    if cfg is not None and not isinstance(cfg, dict):
        raise ValueError(f"Invalid configuration type for id={integration_id}")
    cfg = cfg if isinstance(cfg, dict) else {}

    params = _local_gurucul_params(command)

    for key in SUPABASE_CONFIGURATION_KEYS:
        if key not in cfg:
            continue
        value = cfg[key]
        if value is None or value == "":
            continue
        params[key] = value

    if row.get("tenant_id") not in (None, ""):
        params["tenant_id"] = row["tenant_id"]
    if row.get("instance_name") not in (None, ""):
        params["instance_name"] = row["instance_name"]
    if row.get("tenant_name") not in (None, ""):
        params["tenant_name"] = row["tenant_name"]
    if row.get("logo") not in (None, ""):
        params["alert_source"] = row["logo"]

    print(
        f"[params] supabase id={integration_id} "
        f"url={params.get('url')!r} max_fetch={params.get('max_fetch')!r} "
        f"first_fetch={params.get('first_fetch')!r} "
        f"instance_name={params.get('instance_name')!r} "
        f"tenant_name={params.get('tenant_name')!r}"
    )
    return params


@task(log_prints=True, persist_result=False)
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


@task(log_prints=True, persist_result=False)
def update_last_run_in_supabase(integration_id: int, last_run: Dict[str, Any]) -> None:
    supabase = get_supabase_client()
    supabase.table("integration_instances").update({"last_run": last_run}).eq(
        "id", integration_id
    ).execute()


def _as_jsonb(value: Any, *, empty: Any = None) -> Any:
    """Ensure a value is a JSON-serializable object for jsonb columns (not a string)."""
    if value is None:
        return empty
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return empty
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return value
        return parsed
    return value


@task(log_prints=True, persist_result=False)
def insert_incident_row_in_supabase(incident: Dict[str, Any]) -> None:
    """Insert a single fetched incident into Supabase (``dev_tickets``)."""
    if not SUPABASE_AVAILABLE or create_client is None:
        print("[insert] Supabase client unavailable; skipping incident insert.")
        _log().warning("Supabase client unavailable; skipping incident insert.")
        return
    row = dict(incident)
    row["raw_logs"] = _as_jsonb(row.get("raw_logs"), empty=[])
    row["rawJSON"] = _as_jsonb(row.get("rawJSON"), empty={})
    row["ai_message"] = _as_jsonb(row.get("ai_message"), empty={})
    raw_logs = row.get("raw_logs")
    raw_logs_count = len(raw_logs) if isinstance(raw_logs, list) else (
        "object" if isinstance(raw_logs, dict) else 0
    )
    print(
        f"[insert] inserting source_id={row.get('source_id')!r} "
        f"name={row.get('name')!r} occurred_at={row.get('occurred_at')!r} "
        f"severity={row.get('severity')!r} raw_logs_count={raw_logs_count}"
    )
    try:
        supabase = get_supabase_client()
        response = supabase.table(SUPABASE_DEV_TICKETS_TABLE).insert(row).execute()
        if response.data:
            print(f"[insert] ok source_id={row.get('source_id')!r}")
            _log().info(
                f"Supabase incident insert ok source_id={row.get('source_id')!r}"
            )
        else:
            print(f"[insert] no data returned source_id={row.get('source_id')!r}")
            _log().warning(
                "Supabase incident insert returned no data "
                f"source_id={row.get('source_id')!r}"
            )
    except Exception as exc:  # noqa: BLE001
        print(f"[insert] failed source_id={row.get('source_id')!r}: {exc}")
        _log().warning(
            f"Supabase incident insert failed source_id={row.get('source_id')!r}: {exc}"
        )


# ---------------------------------------------------------------------------
# Prefect flow result storage (S3Bucket block)
# ---------------------------------------------------------------------------
# Create once (uses default Boto3 credential chain unless AwsCredentials is set):
#
#   from prefect_aws.s3 import S3Bucket
#   S3Bucket(bucket_name="astra-archives", bucket_folder="prefect-results/gurucul").save(
#       "aws-s3", overwrite=True
#   )
#
# Then reference as ``s3-bucket/<block-name>`` below.
# Override with env ``PREFECT_GURUCUL_RESULTS_S3_BLOCK`` (full slug).
# Tasks use persist_result=False so only the flow snapshot is written to S3
# (task returns like Supabase Client are not JSON-serializable).

_FLOW_RESULT_STORAGE = (
    os.environ.get("PREFECT_GURUCUL_RESULTS_S3_BLOCK") or "s3-bucket/aws-s3"
).strip()

# Caller retrieval after run_deployment (Prefect 3.8):
#   final_state = flow_run.state
#   snapshot = final_state.result(raise_on_failure=True)
# Caller needs AWS read access to the same S3 bucket as the block.


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


@flow(
    log_prints=True,
    persist_result=True,
    result_storage=_FLOW_RESULT_STORAGE,
    result_serializer="json",
)
def main(
    integration_id: int = None,
    command: str = None,
    args: Optional[Dict[str, Any]] = None,
    argue: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Run the integration using Supabase-backed params (merged with local defaults).

    Returns a JSON-serializable snapshot persisted via the configured
    ``S3Bucket`` block (``persist_result=True``).

    For ``health-check``, pass check parameters via ``args`` or ``argue`` (alias):
    ``query``, ``match_field``, ``health_check_list``, ``assessment_check``,
    ``mail_notification_group``, ``chat_notification_group``, etc.
    """
    if integration_id is None:
        raise ValueError(
            "Integration ID is required. Usage: main(integration_id=1, command='fetch-incidents')"
        )

    resolved_command = command or "test-module"
    merged_args = dict(args or {})
    if argue:
        merged_args.update(argue)
    payload = {
        "command": resolved_command,
        "params": get_supabase_params(integration_id, resolved_command),
        "args": merged_args,
        "state": {
            "last_run": get_last_run_from_supabase(integration_id),
            "integration_context": {},
        },
        "log_level": "INFO",
    }

    runtime_ctx = RuntimeContext.from_payload(payload)
    init(runtime_ctx)

    params = runtime_ctx.params
    arguments = runtime_ctx.args
    cmd = runtime_ctx.command
    log = runtime_ctx.logger

    try:
        api_key = params.get("apikey")
        base_url = urljoin(params["url"], "/api/")
        verify_certificate = not params.get("insecure", False)
        first_fetch_time = arg_to_timestamp(
            arg=params.get("first_fetch", "1 days"),
            arg_name="First fetch time",
            required=True,
        )
        assert isinstance(first_fetch_time, int)
        proxy = params.get("proxy", False)
        page = arguments.get("page", "1")
        page_count_no = arguments.get("max", "25")
        log.debug(f"Command being called is {cmd}")
        page_params = {"page": page, "max": page_count_no}
        headers = {"Authorization": f"Bearer {api_key}"}
        client = Client(base_url=base_url, verify=verify_certificate, headers=headers, proxy=proxy)

        if cmd == "test-module":
            try:
                result = test_module_command(client)
                return_results(result)
            except Exception:
                return_error(
                    "Gurucul services are currently not available. Please contact the administrator for further assistance."
                )

        elif cmd == "gra-validate-api":
            try:
                result = client.validate_api_key()
                return_results(result)
            except Exception:
                return_error("Error in service")

        elif cmd == "gra-search":
            events = gra_search_command(client, arguments)
            return_results(
                CommandResults(
                    outputs_prefix="Gra.Search",
                    outputs_key_field="",
                    outputs=events,
                    raw_response=events,
                )
            )

        elif cmd == "health-check":
            summary = gra_health_check_command(
                client,
                instance_id=integration_id,
                params=params,
                check_args=arguments,
            )
            return_results(
                CommandResults(
                    outputs_prefix="Gra.HealthCheck",
                    outputs_key_field="instance_id",
                    outputs=summary,
                    raw_response=summary,
                )
            )

        elif cmd == "fetch-incidents":
            max_results = arg_to_int(
                arg=params.get("max_fetch"), arg_name="max_fetch", required=False
            )
            if not max_results:
                max_results = MAX_INCIDENTS_TO_FETCH
            max_results = max(1, max_results)

            raw_logs_fetch_size = arg_to_int(
                arg=params.get("raw_logs_fetch_size"),
                arg_name="raw_logs_fetch_size",
                required=False,
            )
            if not raw_logs_fetch_size:
                raw_logs_fetch_size = 25
            raw_logs_fetch_size = max(1, min(raw_logs_fetch_size, 500))

            severity_thresholds = _resolve_severity_thresholds(params)
            export_defaults = _export_defaults_from_params(params)

            next_run, incidents = fetch_incidents(
                client=client,
                max_results=max_results,
                last_run=runtime_ctx.state.get_last_run(),
                first_fetch_time=first_fetch_time,
                raw_logs_fetch_size=raw_logs_fetch_size,
                severity_thresholds=severity_thresholds,
                export_defaults=export_defaults,
            )
            runtime_ctx.state.set_last_run(next_run)
            runtime_ctx.output.emit_incidents(incidents)
            print(f"[insert] inserting {len(incidents)} incident(s) into dev_tickets")
            for i, inc in enumerate(incidents, start=1):
                print(f"[insert] ({i}/{len(incidents)})")
                insert_incident_row_in_supabase(inc)
            print("[insert] batch complete")

        elif cmd == "gra-fetch-users":
            fetch_records(client, "/users", "Gra.Users", "employeeId", page_params)

        elif cmd == "gra-fetch-accounts":
            fetch_records(client, "/accounts", "Gra.Accounts", "id", page_params)

        elif cmd == "gra-fetch-active-resource-accounts":
            resource_name = arguments.get("resource_name", "Windows Security")
            active_resource_url = "/resources/" + resource_name + "/accounts"
            fetch_records(client, active_resource_url, "Gra.Active.Resource.Accounts", "id", page_params)

        elif cmd == "gra-fetch-user-accounts":
            employee_id = arguments.get("employee_id")
            user_account_url = "/users/" + employee_id + "/accounts"
            fetch_records(client, user_account_url, "Gra.User.Accounts", "id", page_params)

        elif cmd == "gra-fetch-resource-highrisk-accounts":
            res_name = arguments.get("Resource_name", "Windows Security")
            high_risk_account_resource_url = "/resources/" + res_name + "/accounts/highrisk"
            fetch_records(client, high_risk_account_resource_url, "Gra.Resource.Highrisk.Accounts", "id", page_params)

        elif cmd == "gra-fetch-hpa":
            fetch_records(client, "/accounts/highprivileged", "Gra.Hpa", "id", page_params)

        elif cmd == "gra-fetch-resource-hpa":
            resource_name = arguments.get("Resource_name", "Windows Security")
            resource_hpa = "/resources/" + resource_name + "/accounts/highprivileged"
            fetch_records(client, resource_hpa, "Gra.Resource.Hpa", "id", page_params)

        elif cmd == "gra-fetch-orphan-accounts":
            fetch_records(client, "/accounts/orphan", "Gra.Orphan.Accounts", "id", page_params)

        elif cmd == "gra-fetch-resource-orphan-accounts":
            resource_name = arguments.get("resource_name", "Windows Security")
            resource_orphan = "/resources/" + resource_name + "/accounts/orphan"
            fetch_records(client, resource_orphan, "Gra.Resource.Orphan.Accounts", "id", page_params)

        elif cmd == "gra-user-activities":
            employee_id = arguments.get("employee_id")
            user_activities_url = "/user/" + employee_id + "/activity"
            fetch_records(client, user_activities_url, "Gra.User.Activity", "employee_id", page_params)

        elif cmd == "gra-fetch-users-details":
            employee_id = arguments.get("employee_id")
            fetch_records(client, "/users/" + employee_id, "Gra.User", "employeeId", page_params)

        elif cmd == "gra-highRisk-users":
            fetch_records(client, "/users/highrisk", "Gra.Highrisk.Users", "employeeId", page_params)

        elif cmd == "gra-cases":
            status = arguments.get("status")
            cases_url = "/cases/" + status
            fetch_records(client, cases_url, "Gra.Cases", "caseId", page_params)

        elif cmd == "gra-user-anomalies":
            employee_id = arguments.get("employee_id")
            anomaly_url = "/users/" + employee_id + "/anomalies/"
            fetch_records(client, anomaly_url, "Gra.User.Anomalies", "anomaly_name", page_params)

        elif cmd == "gra-case-action":
            action = arguments.get("action")
            caseId = arguments.get("caseId")
            subOption = arguments.get("subOption")
            caseComment = arguments.get("caseComment")
            riskAcceptDate = arguments.get("riskAcceptDate")
            cases_url = "/cases/" + action
            if action == "riskManageCase":
                post_url = {
                    "caseId": int(caseId),
                    "subOption": subOption,
                    "caseComment": caseComment,
                    "riskAcceptDate": riskAcceptDate,
                }
            else:
                post_url = {"caseId": int(caseId), "subOption": subOption, "caseComment": caseComment}
            post_url_json = json.dumps(post_url)
            fetch_post_records(client, cases_url, "Gra.Case.Action", "caseId", page_params, post_url_json)

        elif cmd == "gra-case-action-anomaly":
            action = arguments.get("action")
            caseId = arguments.get("caseId")
            anomalyNames = arguments.get("anomalyNames")
            subOption = arguments.get("subOption")
            caseComment = arguments.get("caseComment")
            riskAcceptDate = arguments.get("riskAcceptDate")
            cases_url = "/cases/" + action
            if action == "riskAcceptCaseAnomaly":
                post_url = {
                    "caseId": int(caseId),
                    "anomalyNames": anomalyNames,
                    "subOption": subOption,
                    "caseComment": caseComment,
                    "riskAcceptDate": riskAcceptDate,
                }
            else:
                post_url = {
                    "caseId": int(caseId),
                    "anomalyNames": anomalyNames,
                    "subOption": subOption,
                    "caseComment": caseComment,
                }
            post_url_json = json.dumps(post_url)
            fetch_post_records(client, cases_url, "Gra.Cases.Action.Anomaly", "caseId", page_params, post_url_json)

        elif cmd == "gra-investigate-anomaly-summary":
            fromDate = arguments.get("fromDate")
            toDate = arguments.get("toDate")
            modelName = arguments.get("modelName")
            if fromDate is not None and toDate is not None:
                investigateAnomaly_url = (
                    "/investigateAnomaly/anomalySummary/"
                    + modelName
                    + "?fromDate="
                    + fromDate
                    + " 00:00:00&toDate="
                    + toDate
                    + " 23:59:59"
                )
            else:
                investigateAnomaly_url = "/investigateAnomaly/anomalySummary/" + modelName
            fetch_records(client, investigateAnomaly_url, "Gra.Investigate.Anomaly.Summary", "modelId", page_params)

        elif cmd == "gra-analytical-features-entity-value":
            fromDate = arguments.get("fromDate")
            toDate = arguments.get("toDate")
            modelName = arguments.get("modelName")
            entityValue = arguments.get("entityValue")
            entityTypeId = arguments.get("entityTypeId")
            if fromDate is not None and toDate is not None:
                analyticalFeatures_url = (
                    "profile/analyticalFeatures/"
                    + entityValue
                    + "?fromDate="
                    + fromDate
                    + " 00:00:00&toDate="
                    + toDate
                    + " 23:59:59&modelName="
                    + modelName
                )
            else:
                analyticalFeatures_url = "profile/analyticalFeatures/" + entityValue + "?modelName=" + modelName
            if entityTypeId is not None:
                analyticalFeatures_url += "&entityTypeId=" + entityTypeId
            fetch_records(client, analyticalFeatures_url, "Gra.Analytical.Features.Entity.Value", "entityID", page_params)

        elif cmd == "gra-cases-anomaly":
            caseId = arguments.get("caseId")
            anomaliesUrl = "/anomalies/" + caseId
            fetch_records(client, anomaliesUrl, "Gra.Cases.anomalies", "caseId", page_params)

        else:
            print(f"[main] Unknown command: {cmd!r} — nothing executed")
            runtime_ctx.output.emit_error(f"Unknown command: {cmd!r}", raise_after=False)

    except IntegrationError:
        raise
    except Exception as e:
        log.error(traceback.format_exc())
        runtime_ctx.output.emit_error(
            f"Failed to execute {cmd} command.\nError:\n{e!s}",
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

    return runtime_ctx.snapshot()


if __name__ in ("__main__", "__builtin__", "builtins"):
    try:
        integration_id = 15
        command = "health-check"
        argue = {
            "query": "your GRA search expression",
            "match_field": "hostname",
            "health_check_list": [
                {"type": "Firewall", "value": "HOST1", "severity": "High"},
            ],
            "mail_notification_group": "soc-alerts",
            "chat_notification_group": "soc-chat",
            "assessment_check": 42,
            "tenant_id": "7ebe0e31-220e-4ae9-bdc3-9f2a51d17a35",
        }

        ctx = main(
            integration_id=integration_id,
            command=command,
            argue=argue,
        )
        print(json.dumps(ctx, default=str, indent=2))
    except Exception as e:
        print(f"Script execution failed: {e}")
        traceback.print_exc()
