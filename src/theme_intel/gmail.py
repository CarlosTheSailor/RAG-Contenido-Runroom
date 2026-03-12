from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

from src.config import Settings

from .models import SourceDocumentInput
from .utils import clean_newsletter_text, extract_links


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
    text_body = _extract_plain_text(payload.get("payload"))
    cleaned = clean_newsletter_text(text_body)
    links = extract_links(text_body)

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


def _extract_plain_text(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""

    body = payload.get("body")
    if isinstance(body, dict):
        data = body.get("data")
        if isinstance(data, str) and data:
            decoded = _decode_base64url(data)
            if decoded.strip():
                mime = str(payload.get("mimeType") or "")
                if mime.startswith("text/plain"):
                    return decoded

    parts = payload.get("parts")
    if isinstance(parts, list):
        # Prefer text/plain parts first.
        plain_candidates: list[str] = []
        html_candidates: list[str] = []
        for part in parts:
            extracted = _extract_plain_text(part)
            if extracted.strip():
                mime = ""
                if isinstance(part, dict):
                    mime = str(part.get("mimeType") or "")
                if mime.startswith("text/plain"):
                    plain_candidates.append(extracted)
                else:
                    html_candidates.append(extracted)
        if plain_candidates:
            return "\n".join(plain_candidates).strip()
        if html_candidates:
            return "\n".join(html_candidates).strip()

    data = ""
    if isinstance(body, dict):
        raw = body.get("data")
        if isinstance(raw, str):
            data = _decode_base64url(raw)
    return data


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
