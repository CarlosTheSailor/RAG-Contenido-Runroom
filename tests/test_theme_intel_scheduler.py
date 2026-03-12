from __future__ import annotations

import unittest
from datetime import datetime, time, timezone

from src.theme_intel.scheduling import compute_next_run_at_utc, parse_run_time_local


class ThemeIntelSchedulerTests(unittest.TestCase):
    def test_parse_run_time_local_accepts_hhmm_and_hhmmss(self) -> None:
        self.assertEqual(parse_run_time_local("09:30"), time(9, 30))
        self.assertEqual(parse_run_time_local("09:30:00"), time(9, 30))

    def test_compute_next_without_last_uses_today_if_not_passed(self) -> None:
        now_utc = datetime(2026, 1, 10, 8, 0, tzinfo=timezone.utc)  # 09:00 Madrid in winter
        next_run = compute_next_run_at_utc(
            every_n_days=2,
            run_time_local=time(10, 0),
            timezone_name="Europe/Madrid",
            now_utc=now_utc,
            last_run_at_utc=None,
        )
        self.assertEqual(next_run, datetime(2026, 1, 10, 9, 0, tzinfo=timezone.utc))

    def test_compute_next_without_last_uses_tomorrow_if_passed(self) -> None:
        now_utc = datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc)  # 13:00 Madrid
        next_run = compute_next_run_at_utc(
            every_n_days=2,
            run_time_local=time(10, 0),
            timezone_name="Europe/Madrid",
            now_utc=now_utc,
            last_run_at_utc=None,
        )
        self.assertEqual(next_run, datetime(2026, 1, 11, 9, 0, tzinfo=timezone.utc))

    def test_compute_next_with_last_run_respects_every_n_days(self) -> None:
        now_utc = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)
        last_run_utc = datetime(2026, 1, 12, 8, 30, tzinfo=timezone.utc)
        next_run = compute_next_run_at_utc(
            every_n_days=2,
            run_time_local=time(9, 30),
            timezone_name="Europe/Madrid",
            now_utc=now_utc,
            last_run_at_utc=last_run_utc,
        )
        self.assertEqual(next_run, datetime(2026, 1, 16, 8, 30, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
