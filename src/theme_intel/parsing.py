from __future__ import annotations

import json
import re
from typing import Any

from .models import ExtractedTheme, ExtractedThemesPayload, ThemeEvidence


def parse_themes_json(raw_text: str) -> ExtractedThemesPayload:
    parsed = _normalize_themes_payload(_extract_json_candidate(raw_text))
    if parsed is None:
        raise ValueError("No se pudo parsear temas_prioritarios_newsletters del output del modelo.")

    temas = [theme for theme in (_normalize_theme(item) for item in parsed) if theme is not None]
    if not temas:
        raise ValueError("La salida del modelo no contiene temas validos.")

    warnings: list[str] = []
    if len(temas) < 8 or len(temas) > 12:
        warnings.append(f"El modelo devolvio {len(temas)} temas; se esperaba entre 8 y 12.")

    return ExtractedThemesPayload(temas=temas, warnings=warnings)


def _extract_json_candidate(raw_text: str) -> Any:
    clean = _strip_code_fences(raw_text)
    direct = _safe_json_loads(clean)
    if direct is not None:
        return direct

    first_obj = clean.find("{")
    last_obj = clean.rfind("}")
    if first_obj != -1 and last_obj > first_obj:
        wrapped = _safe_json_loads(clean[first_obj : last_obj + 1])
        if wrapped is not None:
            return wrapped

    first_arr = clean.find("[")
    last_arr = clean.rfind("]")
    if first_arr != -1 and last_arr > first_arr:
        array_candidate = _safe_json_loads(clean[first_arr : last_arr + 1])
        if array_candidate is not None:
            return array_candidate

    raise ValueError("Output no parseable como JSON.")


def _normalize_themes_payload(data: Any) -> list[Any] | None:
    if isinstance(data, dict):
        temas = data.get("temas_prioritarios_newsletters")
        if isinstance(temas, list):
            return temas
        temas_alt = data.get("temas")
        if isinstance(temas_alt, list):
            return temas_alt
        posts = data.get("data")
        if isinstance(posts, dict):
            return _normalize_themes_payload(posts)
        return None

    if isinstance(data, list):
        return data
    return None


def _normalize_theme(item: Any) -> ExtractedTheme | None:
    if not isinstance(item, dict):
        return None

    tema = _safe_str(item.get("tema"))
    if not tema:
        return None

    contexto = _safe_str(item.get("contexto_newsletters"))
    keywords_raw = item.get("keywords")
    keywords = [_safe_str(value) for value in keywords_raw] if isinstance(keywords_raw, list) else []
    keywords = [value for value in keywords if value]

    evidences_raw = item.get("datos_cuantitativos_relacionados")
    evidences = [_normalize_evidence(value) for value in evidences_raw] if isinstance(evidences_raw, list) else []
    evidences = [value for value in evidences if value is not None]

    return ExtractedTheme(
        tema=tema,
        contexto_newsletters=contexto,
        keywords=keywords,
        datos_cuantitativos_relacionados=evidences,
    )


def _normalize_evidence(item: Any) -> ThemeEvidence | None:
    if not isinstance(item, dict):
        return None

    return ThemeEvidence(
        dato=_safe_str(item.get("dato")),
        fuente=_safe_str(item.get("fuente")) or "Fuente no especificada en newsletter",
        texto_fuente_breve=_safe_str(item.get("texto_fuente_breve")),
        url_referencia=_safe_str(item.get("url_referencia")),
        newsletter_origen=_safe_str(item.get("newsletter_origen")) or "Newsletter no identificada",
    )


def _strip_code_fences(text: str) -> str:
    output = text.strip()
    output = re.sub(r"^```json\s*", "", output, flags=re.IGNORECASE)
    output = re.sub(r"^```\s*", "", output)
    output = re.sub(r"\s*```$", "", output)
    return output.strip()


def _safe_json_loads(text: str) -> Any | None:
    try:
        return json.loads(text)
    except Exception:
        return None


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()
