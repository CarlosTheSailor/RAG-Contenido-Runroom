from __future__ import annotations

from pathlib import Path


class ThemeIntelPromptLoader:
    def __init__(self, assets_dir: Path):
        self._assets_dir = assets_dir
        self._system_path = assets_dir / "prompts" / "temas_system.txt"
        self._user_path = assets_dir / "prompts" / "temas_user.txt"

    def load_system_prompt(self) -> str:
        return _load_text_file(self._system_path)

    def load_user_prompt_template(self) -> str:
        return _load_text_file(self._user_path)


def _load_text_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing prompt file: {path}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"Prompt file is empty: {path}")
    return text
