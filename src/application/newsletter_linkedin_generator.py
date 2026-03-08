from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from src.config import Settings


@dataclass(frozen=True)
class NewsletterLinkedInInput:
    idea: str
    referencias: str | None = None
    audiencia: str | None = None
    objetivo_secundario: str | None = None
    longitud: str | None = None
    metafora_visual: str | None = None
    texto_a_incluir: str | None = None


@dataclass(frozen=True)
class NewsletterLinkedInResult:
    output_text: str
    related_content: list[dict[str, Any]]
    warnings: list[str]
    used_examples: list[str]


class NewsletterLinkedInGenerator:
    def __init__(self, settings: Settings, assets_dir: Path):
        self._settings = settings
        self._assets_dir = assets_dir
        self._prompt_path = assets_dir / "prompts" / "base_prompt.txt"
        self._examples_dir = assets_dir / "examples"

    def load_base_prompt(self) -> str:
        if not self._prompt_path.exists():
            raise FileNotFoundError(f"Missing prompt file: {self._prompt_path}")
        text = self._prompt_path.read_text(encoding="utf-8").strip()
        if not text:
            raise ValueError(f"Prompt file is empty: {self._prompt_path}")
        return text

    def load_style_examples(self) -> list[tuple[str, str]]:
        if not self._examples_dir.exists():
            return []
        output: list[tuple[str, str]] = []
        for path in sorted(self._examples_dir.glob("*.txt"), key=lambda item: item.name.lower()):
            text = path.read_text(encoding="utf-8").strip()
            if text:
                output.append((path.name, text))
        return output

    def generate(
        self,
        payload: NewsletterLinkedInInput,
        related_content: Sequence[dict[str, Any]] | None = None,
        force_offline: bool = False,
    ) -> NewsletterLinkedInResult:
        base_prompt = self.load_base_prompt()
        style_examples = self.load_style_examples()
        normalized_related = _normalize_related_content(related_content or [])

        user_prompt = build_newsletter_generation_prompt(
            payload=payload,
            related_content=normalized_related,
            style_examples=style_examples,
        )

        warnings: list[str] = []
        output_text = self._generate_with_llm(
            base_prompt=base_prompt,
            user_prompt=user_prompt,
            force_offline=force_offline,
        )
        if output_text is None:
            output_text = _fallback_output(payload=payload, related_content=normalized_related)
            warnings.append("No se pudo generar con OpenAI. Se devolvio una version fallback.")

        return NewsletterLinkedInResult(
            output_text=output_text,
            related_content=normalized_related,
            warnings=warnings,
            used_examples=[name for name, _ in style_examples],
        )

    def _generate_with_llm(self, base_prompt: str, user_prompt: str, force_offline: bool = False) -> str | None:
        if force_offline or not self._settings.openai_api_key:
            return None

        model = self._settings.openai_newsletter_model or self._settings.openai_metadata_model
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": base_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.35,
        }

        try:
            raw = _post_openai_chat(
                base_url=self._settings.openai_base_url,
                api_key=self._settings.openai_api_key,
                payload=payload,
            )
            content = str(raw["choices"][0]["message"]["content"]).strip()
            if not content:
                return None
            return content
        except Exception:
            return None


def build_newsletter_generation_prompt(
    payload: NewsletterLinkedInInput,
    related_content: Sequence[dict[str, Any]],
    style_examples: Sequence[tuple[str, str]],
) -> str:
    lines: list[str] = []
    lines.append("Genera el contenido final en texto plano usando el formato solicitado.")
    lines.append("")
    lines.append("ENTRADAS DEL USUARIO")
    lines.append(f"IDEA: {payload.idea.strip()}")

    optional_fields = [
        ("REFERENCIAS", payload.referencias),
        ("AUDIENCIA", payload.audiencia),
        ("OBJETIVO_SECUNDARIO", payload.objetivo_secundario),
        ("LONGITUD", payload.longitud),
        ("METAFORA_VISUAL", payload.metafora_visual),
        ("TEXTO_A_INCLUIR_INTEGRAMENTE", payload.texto_a_incluir),
    ]
    for key, value in optional_fields:
        cleaned = (value or "").strip()
        if cleaned:
            lines.append(f"{key}: {cleaned}")

    lines.append("")
    lines.append("INSTRUCCIONES EXTRA PARA ESTA EJECUCION")
    lines.append("Integra con criterio 1-3 referencias del RAG dentro del articulo si aportan contexto real.")
    lines.append("Al final anade una seccion 'Referencias relacionadas' con URLs efectivamente usadas.")
    lines.append("Si una referencia no aporta, no la fuerces.")

    lines.append("")
    lines.append("CONTENIDO RELACIONADO DEL RAG")
    if not related_content:
        lines.append("Sin resultados de RAG para esta ejecucion.")
    else:
        for idx, item in enumerate(related_content, start=1):
            title = str(item.get("title") or "").strip()
            ctype = str(item.get("content_type") or "").strip() or "other"
            score = float(item.get("score") or 0.0)
            url = str(item.get("url") or "").strip() or "(sin URL)"
            excerpt = str(item.get("excerpt") or "").strip()
            lines.append(f"{idx}. [{ctype}] {title}")
            lines.append(f"URL: {url}")
            lines.append(f"Score: {score:.4f}")
            if excerpt:
                lines.append(f"Extracto: {excerpt}")
            lines.append("")

    lines.append("EJEMPLOS DE ESTILO (FEW-SHOT)")
    if not style_examples:
        lines.append("No hay ejemplos cargados.")
    else:
        for name, text in style_examples:
            lines.append(f"=== EJEMPLO {name} ===")
            lines.append(text)
            lines.append("")

    return "\n".join(lines).strip()


def _normalize_related_content(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        title = str(row.get("title") or "").strip()
        if not title:
            continue

        url_raw = row.get("url")
        url = str(url_raw).strip() if isinstance(url_raw, str) and url_raw.strip() else None
        content_type = str(row.get("content_type") or "other").strip() or "other"

        try:
            score = round(float(row.get("score") or 0.0), 6)
        except (TypeError, ValueError):
            score = 0.0

        excerpt = ""
        chunks = row.get("matched_chunks")
        if isinstance(chunks, list) and chunks:
            first = chunks[0]
            if isinstance(first, dict):
                excerpt = str(first.get("text") or "").strip()
        if len(excerpt) > 280:
            excerpt = excerpt[:277].rstrip() + "..."

        normalized.append(
            {
                "title": title,
                "url": url,
                "content_type": content_type,
                "score": score,
                "excerpt": excerpt,
            }
        )
    return normalized


def _post_openai_chat(base_url: str, api_key: str, payload: dict[str, Any]) -> dict[str, Any]:
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


def _fallback_output(payload: NewsletterLinkedInInput, related_content: Sequence[dict[str, Any]]) -> str:
    idea = payload.idea.strip()
    title_short = "Cambiar el sistema, no el titular"
    title_metaphor = "Cuando el tablero manda mas que el juego"
    title_sober = "Incentivos, decisiones y resultados"

    references_lines: list[str] = []
    for row in related_content:
        title = str(row.get("title") or "").strip()
        url = str(row.get("url") or "").strip()
        if title and url:
            references_lines.append(f"{title}: {url}")

    references_block = "\n".join(references_lines) if references_lines else "No hay referencias disponibles en esta ejecucion."

    article_a = (
        f"Hay ideas que parecen de ejecucion y en realidad son de diseno del sistema. {idea}. "
        "Cuando una organizacion optimiza actividad en lugar de impacto, el resultado suele ser previsible: mas movimiento, menos aprendizaje. "
        "No es un fallo de actitud individual; es una consecuencia logica de incentivos mal alineados.\n\n"
        "La friccion aparece en decisiones pequenas: que se prioriza, que se mide, que se premia y que se ignora. "
        "Si el sistema recompensa output, el equipo produce output. "
        "Si queremos outcomes, hay que redisenar las reglas del juego y no solo pedir mas compromiso.\n\n"
        "La pregunta no es si trabajamos mucho. La pregunta es si estamos aprendiendo lo correcto para mover resultados reales."
    )

    article_b = (
        f"Tomemos esta idea como punto de partida: {idea}. "
        "En producto y estrategia, muchas discusiones se atascan porque mezclamos actividad con impacto y suponemos causalidad donde solo hay correlacion. "
        "Separar esas capas mejora la calidad de las decisiones.\n\n"
        "Primero conviene identificar el mecanismo: que incentivos empujan el comportamiento observado y que metricas lo refuerzan. "
        "Despues, traducirlo a cambios de comportamiento que podamos observar en usuarios, equipos y negocio. "
        "Sin ese puente, cualquier plan se queda en intencion.\n\n"
        "Cuando el marco esta claro, la ejecucion deja de ser una carrera de entregables y pasa a ser un sistema de aprendizaje aplicado."
    )

    teaser = (
        "Seguimos confundiendo actividad con impacto.\n"
        "Nos tranquiliza ver movimiento en el dashboard.\n"
        "Pero movimiento no es progreso.\n"
        "Cuando el sistema premia output, obtienes output.\n"
        "Cuando premia aprendizaje util, empiezas a cambiar resultados.\n"
        "En la newsletter de esta semana desarrollo esta idea con ejemplos concretos.\n"
        "Que indicador te esta haciendo tomar peores decisiones sin que lo parezca?"
    )

    return (
        "=== ARTICULO | VARIANTE A (Provocacion util) ===\n"
        "Titulos (3):\n"
        f"{title_short}\n"
        f"{title_metaphor}\n"
        f"{title_sober}\n\n"
        f"{article_a}\n\n"
        "Te leo en comentarios 👇\n"
        "Si te aporta, puedes suscribirte a la newsletter.\n\n"
        "=== ARTICULO | VARIANTE B (Didactica clara) ===\n"
        "Titulos (3):\n"
        f"{title_short}\n"
        f"{title_metaphor}\n"
        f"{title_sober}\n\n"
        f"{article_b}\n\n"
        "Te leo en comentarios 👇\n"
        "Si te aporta, puedes suscribirte a la newsletter.\n\n"
        "=== PROMPT PARA IMAGEN (CABECERA) ===\n"
        "Ilustracion 2D minimalista sin texto: un panel de metricas con muchas agujas moviendose y, al fondo, una unica ruta clara iluminada. "
        "Paleta sobria, composicion limpia, metafora de actividad vs impacto.\n\n"
        "=== POST LINKEDIN (TEASER) ===\n"
        f"{teaser}\n\n"
        "Referencias relacionadas\n"
        f"{references_block}"
    )
