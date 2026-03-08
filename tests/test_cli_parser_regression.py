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


if __name__ == "__main__":
    unittest.main()
