from __future__ import annotations

from pathlib import Path


class LinkedInDraftPromptLoader:
    def __init__(self, assets_dir: Path):
        self._assets_dir = assets_dir
        self._prompt_dir = assets_dir / "prompts"

    def load(self, name: str) -> str:
        return _load_text_file(self._prompt_dir / name)

    def load_topic_selection_system(self) -> str:
        return self.load("topic_selection_system.txt")

    def load_topic_selection_user(self) -> str:
        return self.load("topic_selection_user.txt")

    def load_draft_stage1_system(self) -> str:
        return self.load("draft_stage1_system.txt")

    def load_draft_stage1_user(self) -> str:
        return self.load("draft_stage1_user.txt")

    def load_draft_stage2_refine_system(self) -> str:
        return self.load("draft_stage2_refine_system.txt")

    def load_draft_stage2_refine_user(self) -> str:
        return self.load("draft_stage2_refine_user.txt")


def _load_text_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing prompt file: {path}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"Prompt file is empty: {path}")
    return text
