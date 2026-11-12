"""
This tool searches Gurucul GRA logs within a time range and supports filtering,
aggregation, and grouping. Prefer group-by exploration before narrow filters.

Args:
    integration_id: Gurucul instance id (Supabase integration_instances.id).
    query: Gurucul GRA search query, for example
     1. group by datasourcename  # explore datasources available
     2. datasourcename = "Fortinet" group by logtype  # pivot and explore categories
     3. ((datasourcename = "Fortinet" and logtype = "utm")) and application = "Google.Drive"
     4. datasourcename = "Fortinet" and logtype = "traffic" group by country | count(id)
    from_date: Start of the search window in UTC, format YYYY-MM-DD HH:MM:SS.
    to_date: End of the search window in UTC, format YYYY-MM-DD HH:MM:SS.
    period: Relative window when from_date is not set, e.g. "24 hours", "1 hour".
    page: Page of results (1-based).
    max_events: Records per page (capped at 500).
"""

from __future__ import annotations

import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dateparser
import requests
import wmill
from supabase import create_client

SUPABASE_URL = "https://zhhsijigoupqroztdrdy.supabase.co"
SUPABASE_ANON_KEY_VARIABLE = "u/shubham/supabase_anon_key"

API_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
GURUCUL_RETRIES = 1
GURUCUL_RETRY_DELAY_SEC = 2.0
GURUCUL_HTTP_TIMEOUT = 60
GURUCUL_MAX_EVENTS_CAP = 500


def _variable(path: str) -> str:
    try:
        return (wmill.get_variable(path) or "").strip()
    except Exception:
        return ""


def _supabase_anon_key() -> str:
    key = _variable(SUPABASE_ANON_KEY_VARIABLE) or os.getenv("SUPABASE_ANON_KEY", "").strip()
    if not key:
        raise RuntimeError(
            f"Supabase anon key is not set: configure {SUPABASE_ANON_KEY_VARIABLE} "
            "or env SUPABASE_ANON_KEY"
        )
    return key


def get_gurucul_auth(integration_id: int) -> Dict[str, str]:
    """Load GRA ``url`` and ``apikey`` from Supabase ``integration_instances``."""
    supabase = create_client(SUPABASE_URL, _supabase_anon_key())
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
    cfg = cfg if isinstance(cfg, dict) else {}

    url = (cfg.get("url") or "").strip()
    apikey = (cfg.get("apikey") or "").strip()
    if not url:
        raise ValueError(
            f"integration_instances id={integration_id} has no configuration.url"
        )
    if not apikey:
        raise ValueError(
            f"integration_instances id={integration_id} has no configuration.apikey"
        )

    print(f"[params] supabase id={integration_id} url={url!r}")
    return {"url": url, "apikey": apikey}


def _format_api_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime(API_DATE_FORMAT)


def _coerce_api_datetime(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _format_api_datetime(value)
    if isinstance(value, (int, float)):
        return _format_api_datetime(datetime.fromtimestamp(int(value), tz=timezone.utc))
    text = str(value).strip()
    if not text:
        return None
    try:
        datetime.strptime(text, API_DATE_FORMAT)
        return text
    except ValueError:
        pass
    parsed = dateparser.parse(
        text, settings={"TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True}
    )
    if parsed is None:
        raise ValueError(f"Invalid datetime: {value!r}")
    return _format_api_datetime(parsed)


def resolve_search_time_window(
    *,
    from_date: Any = None,
    to_date: Any = None,
    period: Any = None,
) -> tuple:
    """Resolve eventFromDate/eventToDate (same rules as drx-gurucul)."""
    from_s = _coerce_api_datetime(from_date)
    to_s = _coerce_api_datetime(to_date)
    if from_s or to_s:
        if from_s and not to_s:
            to_s = _format_api_datetime(datetime.now(timezone.utc))
        return from_s, to_s

    if period in (None, ""):
        return None, None
    rel = str(period).strip()
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


def _strip_empty_string_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            k: _strip_empty_string_fields(v)
            for k, v in value.items()
            if v != ""
        }
    if isinstance(value, list):
        return [_strip_empty_string_fields(item) for item in value]
    return value


def _parse_big_data_events(response: Any) -> List[Any]:
    if isinstance(response, dict):
        result = response.get("Result") or []
        events = result if isinstance(result, list) else [result]
    elif isinstance(response, list):
        events = response
    else:
        events = []
    return _strip_empty_string_fields(events)


def search_big_data_events(
    base_url: str,
    api_key: str,
    expression: str,
    page: int = 1,
    max_events: int = 100,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> List[Any]:
    """POST /v1/searchBigDataEvents; retry once when Result is empty."""
    expression = (expression or "").strip()
    if not expression:
        raise ValueError("query/expression is required")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    page = max(1, int(page))
    max_events = max(1, min(int(max_events), GURUCUL_MAX_EVENTS_CAP))
    attempts = GURUCUL_RETRIES + 1

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
        f"[gurucul] expression={expression!r} page={page} max={max_events} "
        f"eventFromDate={form.get('eventFromDate')!r} "
        f"eventToDate={form.get('eventToDate')!r}"
    )

    url = f"{base_url.rstrip('/')}/api/v1/searchBigDataEvents"

    def _once(attempt: int) -> tuple:
        try:
            http_response = requests.post(
                url,
                data=form,
                headers=headers,
                timeout=GURUCUL_HTTP_TIMEOUT,
                verify=False,
            )
            http_response.raise_for_status()
            payload = http_response.json()
            events = _parse_big_data_events(payload)
            if not events:
                print(
                    f"[gurucul] attempt={attempt} empty Result "
                    f"expression={expression!r} response={payload!r}"
                )
            return events, None
        except Exception:
            error = traceback.format_exc()
            print(f"[gurucul] attempt={attempt} error:\n{error}")
            return [], error

    events, error = _once(1)
    for attempt in range(2, attempts + 1):
        if events:
            break
        print(f"[gurucul] events=0 - retrying (attempt {attempt}/{attempts})")
        time.sleep(GURUCUL_RETRY_DELAY_SEC)
        events, error = _once(attempt)

    if not events and error:
        raise RuntimeError(
            "Gurucul search failed (full traceback in the job logs): "
            + error.strip().splitlines()[-1]
        )
    if not events:
        print(f"[gurucul] still 0 events after {attempts} attempt(s) - continuing")
    return events


def main(
    integration_id: int,
    query: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: str = "",
    page: int = 1,
    max_events: int = 100,
) -> List[Any]:
    """Search Gurucul GRA and return Result rows (empty-string fields stripped)."""
    auth = get_gurucul_auth(integration_id)
    from_date, to_date = resolve_search_time_window(
        from_date=from_date,
        to_date=to_date,
        period=period,
    )
    events = search_big_data_events(
        base_url=auth["url"],
        api_key=auth["apikey"],
        expression=query,
        page=page,
        max_events=max_events,
        from_date=from_date,
        to_date=to_date,
    )
    print(f"[gurucul] result_count={len(events)}")
    return events
