from __future__ import annotations

import base64
import html
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

from src.config import Settings

from .models import SourceDocumentInput
from .utils import clean_newsletter_text, extract_links, looks_like_html_fallback_text

_COMMENT_RE = re.compile(r"(?is)<!--.*?-->")
_STRIP_CONTAINER_RE = re.compile(r"(?is)<(script|style|head|title|meta|link|svg|noscript)[^>]*>.*?</\1>")
_BREAK_TAG_RE = re.compile(r"(?is)<\s*br\s*/?\s*>")
_BLOCK_TAG_RE = re.compile(r"(?is)</?\s*(p|div|section|article|aside|header|footer|main|li|ul|ol|table|tr|td|th|blockquote|h[1-6]|hr)[^>]*>")
_TAG_RE = re.compile(r"(?is)<[^>]+>")


class GmailClient:
    def __init__(self, settings: Settings):
        self._settings = settings

    def list_messages(
        self,
        query: str,
        max_results: int,
        on_message_fetched: Callable[[int, int, int], None] | None = None,
    ) -> list[SourceDocumentInput]:
        token = self._access_token()
        labels_map = self._list_labels_map(token=token)
        refs = self._gmail_get(
            token=token,
            path="/users/me/messages",
            params={"q": query, "maxResults": max_results},
        )
        messages_raw = refs.get("messages")
        if not isinstance(messages_raw, list):
            return []

        total = len(messages_raw)
        output: list[SourceDocumentInput] = []
        fetched_count = 0
        for item in messages_raw:
            if not isinstance(item, dict):
                continue
            message_id = str(item.get("id") or "").strip()
            if not message_id:
                continue
            detail = self._gmail_get(
                token=token,
                path=f"/users/me/messages/{message_id}",
                params={"format": "full"},
            )
            parsed = _to_source_document(detail, labels_map=labels_map)
            fetched_count += 1
            if parsed is not None:
                output.append(parsed)
            if on_message_fetched is not None:
                on_message_fetched(fetched_count, total, len(output))
        return output

    def mark_as_read(self, message_ids: list[str]) -> None:
        if not message_ids:
            return
        token = self._access_token()
        for message_id in message_ids:
            clean_id = message_id.strip()
            if not clean_id:
                continue
            self._gmail_post(
                token=token,
                path=f"/users/me/messages/{clean_id}/modify",
                payload={"removeLabelIds": ["UNREAD"]},
            )

    def get_messages(self, message_ids: list[str], ignore_missing: bool = False) -> dict[str, SourceDocumentInput]:
        token = self._access_token()
        labels_map = self._list_labels_map(token=token)
        output: dict[str, SourceDocumentInput] = {}
        for message_id in message_ids:
            clean_id = message_id.strip()
            if not clean_id:
                continue
            try:
                detail = self._gmail_get(
                    token=token,
                    path=f"/users/me/messages/{clean_id}",
                    params={"format": "full"},
                )
            except Exception as exc:
                if ignore_missing and _is_missing_gmail_error(exc):
                    continue
                raise
            parsed = _to_source_document(detail, labels_map=labels_map)
            if parsed is not None:
                output[clean_id] = parsed
        return output

    def _access_token(self) -> str:
        client_id = (self._settings.gmail_oauth_client_id or "").strip()
        client_secret = (self._settings.gmail_oauth_client_secret or "").strip()
        refresh_token = (self._settings.gmail_oauth_refresh_token or "").strip()
        if not (client_id and client_secret and refresh_token):
            raise RuntimeError("Gmail OAuth no configurado. Revisa GMAIL_OAUTH_CLIENT_ID/SECRET/REFRESH_TOKEN.")

        payload = urllib.parse.urlencode(
            {
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            "https://oauth2.googleapis.com/token",
            data=payload,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Gmail OAuth HTTP {exc.code}: {body}") from exc

        token = str(body.get("access_token") or "").strip()
        if not token:
            raise RuntimeError("No se obtuvo access_token de Google OAuth.")
        return token

    def _gmail_get(self, token: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        query = ""
        if params:
            query = "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            f"https://gmail.googleapis.com/gmail/v1{path}{query}",
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Gmail API HTTP {exc.code}: {body}") from exc

    def _gmail_post(self, token: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        req = urllib.request.Request(
            f"https://gmail.googleapis.com/gmail/v1{path}",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Gmail API HTTP {exc.code}: {body}") from exc

    def _list_labels_map(self, token: str) -> dict[str, str]:
        raw = self._gmail_get(token=token, path="/users/me/labels")
        labels = raw.get("labels")
        if not isinstance(labels, list):
            return {}
        mapping: dict[str, str] = {}
        for item in labels:
            if not isinstance(item, dict):
                continue
            label_id = str(item.get("id") or "").strip()
            label_name = str(item.get("name") or "").strip()
            if not label_id or not label_name:
                continue
            mapping[label_id] = label_name
        return mapping


def _to_source_document(payload: dict[str, Any], labels_map: dict[str, str]) -> SourceDocumentInput | None:
    if not isinstance(payload, dict):
        return None

    message_id = str(payload.get("id") or "").strip()
    if not message_id:
        return None

    thread_id = str(payload.get("threadId") or "").strip() or None
    label_ids = payload.get("labelIds")
    labels_raw = [str(item) for item in label_ids] if isinstance(label_ids, list) else []
    labels = _resolve_label_names(labels_raw, labels_map=labels_map)

    internal_date = payload.get("internalDate")
    received_at: datetime | None = None
    if isinstance(internal_date, str) and internal_date.isdigit():
        received_at = datetime.fromtimestamp(int(internal_date) / 1000, tz=timezone.utc)

    headers = _extract_headers(payload.get("payload"))
    subject = headers.get("subject", "")
    sender = headers.get("from", "")
    body = _extract_message_body(payload.get("payload"))
    text_body = body["raw_text"]
    cleaned = clean_newsletter_text(text_body)
    links = extract_links(body["link_source_text"])

    return SourceDocumentInput(
        source_external_id=message_id,
        source_thread_id=thread_id,
        subject=subject,
        sender=sender,
        received_at=received_at,
        labels=labels,
        raw_text=text_body,
        cleaned_text=cleaned,
        links=links,
        metadata={
            "snippet": str(payload.get("snippet") or ""),
            "label_ids": labels_raw,
            "label_names": labels,
            "extraction_mode": body["mode"],
            "plain_candidate_len": len(body["plain_text"]),
            "html_candidate_len": len(body["html_text"]),
            "plain_low_signal": body["plain_low_signal"],
        },
    )


def _extract_headers(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    raw_headers = payload.get("headers")
    if not isinstance(raw_headers, list):
        return {}
    output: dict[str, str] = {}
    for item in raw_headers:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip().lower()
        value = str(item.get("value") or "").strip()
        if name and value:
            output[name] = value
    return output


def _extract_message_body(payload: Any) -> dict[str, Any]:
    candidates: dict[str, list[str]] = {"plain": [], "html": []}
    _collect_mime_candidates(payload, candidates)

    plain_text = "\n".join(item.strip() for item in candidates["plain"] if item.strip()).strip()
    html_source = "\n\n".join(item.strip() for item in candidates["html"] if item.strip()).strip()
    html_text = _html_to_text(html_source)
    plain_low_signal = looks_like_html_fallback_text(plain_text)

    if plain_text and not plain_low_signal:
        raw_text = plain_text
        mode = "plain"
    elif html_text:
        raw_text = html_text
        mode = "html_fallback" if plain_text else "html"
    else:
        raw_text = plain_text or html_text
        mode = "plain"

    link_source_text = "\n".join(part for part in (plain_text, html_source, html_text) if part).strip()
    return {
        "raw_text": raw_text.strip(),
        "mode": mode,
        "plain_text": plain_text,
        "html_text": html_text,
        "plain_low_signal": plain_low_signal,
        "link_source_text": link_source_text,
    }


def _collect_mime_candidates(payload: Any, candidates: dict[str, list[str]]) -> None:
    if not isinstance(payload, dict):
        return

    mime = str(payload.get("mimeType") or "").strip().lower()
    body = payload.get("body")
    if isinstance(body, dict):
        data = body.get("data")
        if isinstance(data, str) and data:
            decoded = _decode_base64url(data).strip()
            if decoded:
                if mime.startswith("text/plain"):
                    candidates["plain"].append(decoded)
                elif mime.startswith("text/html"):
                    candidates["html"].append(decoded)

    parts = payload.get("parts")
    if isinstance(parts, list):
        for part in parts:
            _collect_mime_candidates(part, candidates)


def _html_to_text(html_text: str) -> str:
    if not html_text.strip():
        return ""

    payload = _COMMENT_RE.sub(" ", html_text)
    payload = _STRIP_CONTAINER_RE.sub(" ", payload)
    payload = _BREAK_TAG_RE.sub("\n", payload)
    payload = _BLOCK_TAG_RE.sub("\n", payload)
    payload = _TAG_RE.sub(" ", payload)
    payload = html.unescape(payload).replace("\xa0", " ")

    lines = [re.sub(r"\s+", " ", line).strip() for line in payload.splitlines()]
    compact: list[str] = []
    blank_pending = False
    for line in lines:
        if not line:
            if compact:
                blank_pending = True
            continue
        if blank_pending and compact:
            compact.append("")
        compact.append(line)
        blank_pending = False
    return "\n".join(compact).strip()


def _is_missing_gmail_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "gmail api http 404" in text or "requested entity was not found" in text


def _decode_base64url(value: str) -> str:
    try:
        padding = "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(value + padding)
        return raw.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _resolve_label_names(label_ids: list[str], labels_map: dict[str, str]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for label_id in label_ids:
        label_name = labels_map.get(label_id)
        if not label_name:
            # Fallback to system-like names if already semantic; skip opaque numeric ids.
            fallback = label_id.strip()
            if fallback.isdigit():
                continue
            label_name = fallback
        normalized = label_name.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(normalized)
    return names
