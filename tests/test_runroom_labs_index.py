from __future__ import annotations

import unittest

from src.content.runroom_labs_index import parse_runroom_lab_urls


class RunroomLabsIndexTests(unittest.TestCase):
    def test_parse_runroom_labs_index_selects_one_summary_url_per_group(self) -> None:
        html = """
        <div class="accordion">
          <div class="accordion_group ">
            <div class="accordion_header"><b>LAB A</b></div>
            <div class="accordion_content">
              <p><a href="https://www.youtube.com/watch?v=abc">Mira el video del LAB</a></p>
              <p><a href="/lab-heart-of-agile?hsLang=en">Lee los aprendizajes del LAB</a></p>
              <p><a href="/runroom-lab-noise"></a></p>
            </div>
          </div>

          <div class="accordion_group ">
            <div class="accordion_header"><b>LAB B</b></div>
            <div class="accordion_content">
              <p><a href="https://www.youtube.com/watch?v=def">Mira la charla</a></p>
              <p><a href="https://www.linkedin.com/company/runroom">LinkedIn</a></p>
            </div>
          </div>

          <div class="accordion_group ">
            <div class="accordion_header"><b>LAB C</b></div>
            <div class="accordion_content">
              <p><a href="https://www.runroom.com/realworld/feedback">Aqui</a></p>
            </div>
          </div>

          <div class="accordion_group ">
            <div class="accordion_header"><b>LAB D</b></div>
            <div class="accordion_content">
              <p><a href="https://info.runroom.com/lab-heart-of-agile?hsLang=en">Lee el resumen del evento</a></p>
            </div>
          </div>
        </div>
        """

        urls, stats = parse_runroom_lab_urls(
            index_html=html,
            index_url="https://info.runroom.com/runroom-lab-todas-las-ediciones",
        )

        self.assertEqual(
            urls,
            [
                "https://info.runroom.com/lab-heart-of-agile",
                "https://www.runroom.com/realworld/feedback",
            ],
        )

        self.assertEqual(stats["groups_total"], 4)
        self.assertEqual(stats["groups_with_selected_url"], 3)
        self.assertEqual(stats["groups_without_selected_url"], 1)
        self.assertEqual(stats["duplicates_removed"], 1)


if __name__ == "__main__":
    unittest.main()
