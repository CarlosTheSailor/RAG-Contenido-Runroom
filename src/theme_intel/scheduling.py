from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


def parse_run_time_local(value: str) -> time:
    raw = value.strip()
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.time().replace(microsecond=0)
        except ValueError:
            continue
    raise ValueError("run_time_local invalido. Usa formato HH:MM o HH:MM:SS.")


def validate_timezone_name(timezone_name: str) -> str:
    candidate = timezone_name.strip()
    if not candidate:
        raise ValueError("timezone es obligatorio.")
    try:
        ZoneInfo(candidate)
    except Exception as exc:  # pragma: no cover - platform-specific errors.
        raise ValueError(f"timezone invalida: {candidate}") from exc
    return candidate


def compute_next_run_at_utc(
    *,
    every_n_days: int,
    run_time_local: time,
    timezone_name: str,
    now_utc: datetime,
    last_run_at_utc: datetime | None,
) -> datetime:
    if every_n_days < 1:
        raise ValueError("every_n_days debe ser >= 1.")

    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    tz = ZoneInfo(validate_timezone_name(timezone_name))
    now_local = now_utc.astimezone(tz)

    if last_run_at_utc is None:
        candidate_local = datetime.combine(now_local.date(), run_time_local, tzinfo=tz)
        if candidate_local <= now_local:
            candidate_local += timedelta(days=1)
        return candidate_local.astimezone(timezone.utc)

    if last_run_at_utc.tzinfo is None:
        last_run_at_utc = last_run_at_utc.replace(tzinfo=timezone.utc)
    else:
        last_run_at_utc = last_run_at_utc.astimezone(timezone.utc)

    last_local = last_run_at_utc.astimezone(tz)
    candidate_local = datetime.combine(
        last_local.date() + timedelta(days=every_n_days),
        run_time_local,
        tzinfo=tz,
    )
    while candidate_local <= now_local:
        candidate_local += timedelta(days=every_n_days)
    return candidate_local.astimezone(timezone.utc)
