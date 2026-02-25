from __future__ import annotations

import re
import time
from dataclasses import dataclass

import requests


META_CHARSET_RE = re.compile(
    rb"<meta[^>]+charset=['\"]?([\w\-\d]+)['\"]?",
    flags=re.IGNORECASE,
)


@dataclass(slots=True)
class FetchResult:
    ok: bool
    url: str
    html: str | None = None
    status_code: int | None = None
    charset: str | None = None
    error: str | None = None
    exception_type: str | None = None


def _charset_from_header(content_type: str | None) -> str | None:
    if not content_type:
        return None
    match = re.search(r"charset=([\w\-\d]+)", content_type, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip()


def _charset_from_meta(content: bytes) -> str | None:
    head = content[:4096]
    match = META_CHARSET_RE.search(head)
    if not match:
        return None
    return match.group(1).decode("ascii", errors="ignore").strip()


def _decode_html(content: bytes, content_type: str | None) -> tuple[str | None, str | None]:
    header_charset = _charset_from_header(content_type)
    meta_charset = _charset_from_meta(content)
    candidates = [header_charset, meta_charset, "euc_jp", "cp932", "utf-8"]

    tried: list[str] = []
    for charset in candidates:
        if not charset:
            continue
        key = charset.lower()
        if key in tried:
            continue
        tried.append(key)
        try:
            return content.decode(charset), charset
        except UnicodeDecodeError:
            continue

    return None, None


def fetch_html(
    session: requests.Session,
    url: str,
    *,
    timeout_sec: float,
    user_agent: str,
    retries: int = 2,
    retry_wait_sec: float = 0.4,
) -> FetchResult:
    headers = {"User-Agent": user_agent}
    attempts = retries + 1
    last_exception: Exception | None = None

    for attempt in range(attempts):
        try:
            response = session.get(url, headers=headers, timeout=timeout_sec)
        except requests.RequestException as exc:
            last_exception = exc
            if attempt < attempts - 1:
                time.sleep(retry_wait_sec)
                continue
            return FetchResult(
                ok=False,
                url=url,
                error=str(exc),
                exception_type=type(exc).__name__,
            )

        status_code = response.status_code
        if status_code >= 400:
            return FetchResult(
                ok=False,
                url=url,
                status_code=status_code,
                error=f"HTTP {status_code}",
            )

        html, charset = _decode_html(response.content, response.headers.get("Content-Type"))
        if html is None:
            return FetchResult(
                ok=False,
                url=url,
                status_code=status_code,
                error="Failed to decode HTML with supported encodings.",
            )

        return FetchResult(
            ok=True,
            url=url,
            html=html,
            status_code=status_code,
            charset=charset,
        )

    # Should be unreachable, but kept to satisfy type checker expectations.
    return FetchResult(
        ok=False,
        url=url,
        error=str(last_exception) if last_exception else "Unknown fetch failure",
        exception_type=type(last_exception).__name__ if last_exception else None,
    )
