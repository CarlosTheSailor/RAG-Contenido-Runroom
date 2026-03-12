from __future__ import annotations

import unittest

from src.cli import build_parser


class CliParserRegressionTests(unittest.TestCase):
    def test_parser_keeps_legacy_commands(self) -> None:
        parser = build_parser()
        subparsers_action = next(action for action in parser._actions if action.dest == "command")
        commands = set(subparsers_action.choices.keys())

        self.assertIn("ingest-transcripts", commands)
        self.assertIn("query-similar", commands)
        self.assertIn("recommend-content", commands)
        self.assertIn("review-matches", commands)
        self.assertIn("ingest-runroom-labs", commands)
        self.assertIn("reset-theme-intel", commands)
        self.assertIn("theme-intel-backfill-related", commands)

    def test_query_similar_flags_unchanged(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["query-similar", "--text", "hola", "--top-k", "3", "--offline-mode"])

        self.assertEqual(args.command, "query-similar")
        self.assertEqual(args.text, "hola")
        self.assertEqual(args.top_k, 3)
        self.assertTrue(args.offline_mode)

    def test_recommend_content_supports_text_and_filters(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "recommend-content",
                "--text",
                "newsletter",
                "--content-types",
                "episode,case_study",
                "--lang",
                "es",
                "--group-by-type",
            ]
        )

        self.assertEqual(args.command, "recommend-content")
        self.assertEqual(args.text, "newsletter")
        self.assertEqual(args.content_types, "episode,case_study")
        self.assertEqual(args.lang, "es")
        self.assertTrue(args.group_by_type)

    def test_ingest_runroom_labs_supports_expected_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "ingest-runroom-labs",
                "--index-url",
                "https://info.runroom.com/runroom-lab-todas-las-ediciones",
                "--target-tokens",
                "300",
                "--overlap-tokens",
                "50",
                "--batch-size",
                "16",
                "--offline-mode",
                "--dry-run",
            ]
        )

        self.assertEqual(args.command, "ingest-runroom-labs")
        self.assertEqual(args.index_url, "https://info.runroom.com/runroom-lab-todas-las-ediciones")
        self.assertEqual(args.target_tokens, 300)
        self.assertEqual(args.overlap_tokens, 50)
        self.assertEqual(args.batch_size, 16)
        self.assertTrue(args.offline_mode)
        self.assertTrue(args.dry_run)

    def test_reset_theme_intel_supports_confirmation_and_dry_run(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "reset-theme-intel",
                "--confirm",
                "theme-intel",
                "--dry-run",
            ]
        )

        self.assertEqual(args.command, "reset-theme-intel")
        self.assertEqual(args.confirm, "theme-intel")
        self.assertTrue(args.dry_run)

    def test_theme_intel_backfill_related_flags(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "theme-intel-backfill-related",
                "--origin-category",
                "growth",
                "--days",
                "7",
                "--top-k",
                "10",
                "--offline-mode",
            ]
        )

        self.assertEqual(args.command, "theme-intel-backfill-related")
        self.assertEqual(args.origin_category, "growth")
        self.assertEqual(args.days, 7)
        self.assertEqual(args.top_k, 10)
        self.assertTrue(args.offline_mode)

    def test_theme_intel_backfill_related_accepts_all_categories(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "theme-intel-backfill-related",
                "--origin-category",
                "all",
            ]
        )

        self.assertEqual(args.command, "theme-intel-backfill-related")
        self.assertEqual(args.origin_category, "all")
        self.assertEqual(args.days, 7)
        self.assertEqual(args.top_k, 10)
        self.assertFalse(args.offline_mode)


if __name__ == "__main__":
    unittest.main()
