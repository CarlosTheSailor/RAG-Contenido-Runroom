from __future__ import annotations

import unittest

from src.youtube_preview.utils import extract_youtube_video_id


class YouTubePreviewUtilsTests(unittest.TestCase):
    def test_extract_video_id_from_watch_url(self) -> None:
        video_id = extract_youtube_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
        self.assertEqual(video_id, "dQw4w9WgXcQ")

    def test_extract_video_id_from_short_url(self) -> None:
        video_id = extract_youtube_video_id("https://youtu.be/dQw4w9WgXcQ")
        self.assertEqual(video_id, "dQw4w9WgXcQ")


if __name__ == "__main__":
    unittest.main()
