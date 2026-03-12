from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path

from src.config import Settings

from .models import ExtractedTheme, ExtractedThemesPayload
from .parsing import parse_themes_json
from .prompts import ThemeIntelPromptLoader


class ThemeExtractor:
    def __init__(self, settings: Settings, assets_dir: Path):
        self._settings = settings
        self._prompts = ThemeIntelPromptLoader(assets_dir=assets_dir)

    def extract(
        self,
        newsletters_clean: str,
        origin_category: str,
        gmail_query: str,
        force_offline: bool = False,
    ) -> ExtractedThemesPayload:
        cleaned = newsletters_clean.strip()
        if not cleaned:
            return ExtractedThemesPayload(temas=[], warnings=["No hay contenido de newsletters para analizar."])

        system_prompt = self._prompts.load_system_prompt()
        user_prompt_template = self._prompts.load_user_prompt_template()
        user_prompt = (
            user_prompt_template.replace("{{origin_category}}", origin_category)
            .replace("{{gmail_query}}", gmail_query)
            .replace("{{newsletters_clean}}", cleaned)
        )

        raw = self._extract_with_openai(system_prompt, user_prompt, force_offline=force_offline)
        if raw is None:
            return _fallback_extraction(cleaned, origin_category=origin_category)

        try:
            parsed = parse_themes_json(raw)
            return parsed
        except Exception:
            fallback = _fallback_extraction(cleaned, origin_category=origin_category)
            return ExtractedThemesPayload(
                temas=fallback.temas,
                warnings=["No se pudo parsear la respuesta del modelo. Se uso fallback local.", *fallback.warnings],
            )

    def _extract_with_openai(self, system_prompt: str, user_prompt: str, force_offline: bool) -> str | None:
        if force_offline or not self._settings.openai_api_key:
            return None

        model = self._settings.openai_theme_intel_model or self._settings.openai_metadata_model
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
        }
        try:
            raw = _post_openai_chat(
                base_url=self._settings.openai_base_url,
                api_key=self._settings.openai_api_key,
                payload=payload,
            )
            content = str(raw["choices"][0]["message"]["content"]).strip()
            return content or None
        except Exception:
            return None


def _post_openai_chat(base_url: str, api_key: str, payload: dict[str, object]) -> dict[str, object]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"OpenAI HTTP {exc.code}: {body}") from exc


def _fallback_extraction(newsletters_clean: str, origin_category: str) -> ExtractedThemesPayload:
    lines = [line.strip() for line in newsletters_clean.splitlines() if line.strip()]
    topics: list[ExtractedTheme] = []
    for idx, line in enumerate(lines[:8], start=1):
        title = line
        if len(title) > 90:
            title = title[:87].rstrip() + "..."
        topics.append(
            ExtractedTheme(
                tema=f"{origin_category}: {title}",
                contexto_newsletters=f"Tema inferido por fallback local a partir de la linea {idx}.",
                keywords=[origin_category.lower().strip() or "general"],
                datos_cuantitativos_relacionados=[],
            )
        )
    if not topics:
        return ExtractedThemesPayload(temas=[], warnings=["No fue posible extraer temas en fallback."])
    return ExtractedThemesPayload(temas=topics, warnings=["Extraccion generada en fallback local."])
