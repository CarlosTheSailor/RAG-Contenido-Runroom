from __future__ import annotations

import json
import logging
import math
import re
import urllib.error
import urllib.request
from collections import Counter
from typing import Any

from src.config import Settings

from .normalization import STOPWORDS_ES, normalize_for_match

logger = logging.getLogger(__name__)


class AIClient:
    def __init__(
        self,
        settings: Settings,
        force_offline: bool = False,
        allow_embedding_fallback: bool = True,
    ):
        self.settings = settings
        self.force_offline = force_offline
        self.allow_embedding_fallback = allow_embedding_fallback
        self._online = bool(settings.openai_api_key) and not force_offline

    @property
    def online(self) -> bool:
        return self._online

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        if not self._online:
            return [self._fallback_embedding(t) for t in texts]

        payload = {
            "model": self.settings.openai_embedding_model,
            "input": texts,
        }
        if self.settings.openai_embedding_model.startswith("text-embedding-3"):
            payload["dimensions"] = self.settings.embedding_dim
        try:
            raw = self._post_json("/embeddings", payload)
            vectors = [item["embedding"] for item in raw["data"]]
            return [self._normalize_embedding_dim(v) for v in vectors]
        except Exception as exc:  # pragma: no cover - network fallback
            if not self.allow_embedding_fallback:
                raise RuntimeError(f"Embedding API failed: {exc}") from exc
            logger.warning("Embedding API failed, using fallback embeddings: %s", exc)
            return [self._fallback_embedding(t) for t in texts]

    def chunk_metadata(self, text: str, language: str = "es") -> dict[str, Any]:
        if not text:
            return self._fallback_metadata(text)

        if not self._online:
            return self._fallback_metadata(text)

        schema = {
            "name": "chunk_metadata",
            "schema": {
                "type": "object",
                "properties": {
                    "topic": {"type": "string"},
                    "subtopic": {"type": "string"},
                    "entities": {"type": "array", "items": {"type": "string"}},
                    "intent": {"type": "string"},
                    "summary_short": {"type": "string"},
                    "keywords": {"type": "array", "items": {"type": "string"}},
                    "quote_score": {"type": "number"},
                    "quality_flags": {"type": "array", "items": {"type": "string"}},
                },
                "required": [
                    "topic",
                    "subtopic",
                    "entities",
                    "intent",
                    "summary_short",
                    "keywords",
                    "quote_score",
                    "quality_flags",
                ],
                "additionalProperties": False,
            },
            "strict": True,
        }

        messages = [
            {
                "role": "system",
                "content": (
                    "Analiza un chunk de transcripcion de podcast. "
                    "Responde solo JSON valido segun el schema. "
                    f"Idioma principal: {language}."
                ),
            },
            {
                "role": "user",
                "content": text,
            },
        ]

        payload = {
            "model": self.settings.openai_metadata_model,
            "messages": messages,
            "temperature": 0,
            "response_format": {
                "type": "json_schema",
                "json_schema": schema,
            },
        }

        try:
            raw = self._post_json("/chat/completions", payload)
            content = raw["choices"][0]["message"]["content"]
            data = json.loads(content)
            return data
        except Exception as exc:  # pragma: no cover - network fallback
            logger.warning("Metadata API failed, using fallback metadata: %s", exc)
            return self._fallback_metadata(text)

    def cosine_similarity(self, vec_a: list[float], vec_b: list[float]) -> float:
        dot = sum(a * b for a, b in zip(vec_a, vec_b))
        norm_a = math.sqrt(sum(a * a for a in vec_a))
        norm_b = math.sqrt(sum(b * b for b in vec_b))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def _post_json(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY missing")

        url = f"{self.settings.openai_base_url}{endpoint}"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.settings.openai_api_key}",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"OpenAI HTTP {exc.code}: {body}") from exc

    def _fallback_embedding(self, text: str) -> list[float]:
        dim = self.settings.embedding_dim
        vec = [0.0] * dim
        tokens = normalize_for_match(text).split()
        if not tokens:
            return vec

        for token in tokens:
            idx = hash(token) % dim
            sign = -1.0 if (hash(token + "_s") % 2 == 0) else 1.0
            vec[idx] += sign

        norm = math.sqrt(sum(v * v for v in vec))
        if norm == 0:
            return vec
        return [v / norm for v in vec]

    def _normalize_embedding_dim(self, vector: list[float]) -> list[float]:
        dim = self.settings.embedding_dim
        if len(vector) == dim:
            return vector
        if len(vector) > dim:
            return vector[:dim]
        return vector + [0.0] * (dim - len(vector))

    def _fallback_metadata(self, text: str) -> dict[str, Any]:
        cleaned = normalize_for_match(text)
        words = [w for w in cleaned.split() if w and w not in STOPWORDS_ES]
        freq = Counter(words)
        top_keywords = [word for word, _ in freq.most_common(6)]

        summary = text.strip().replace("\n", " ")
        if len(summary) > 180:
            summary = summary[:177].rstrip() + "..."

        entities = self._extract_entities(text)
        intent = "informativo"
        if "?" in text:
            intent = "pregunta"
        if "paso" in cleaned or "como" in cleaned:
            intent = "explicativo"

        quality_flags: list[str] = []
        if "inaudible" in cleaned:
            quality_flags.append("audio_noise")
        if len(words) < 12:
            quality_flags.append("short_chunk")

        topic = top_keywords[0] if top_keywords else "general"
        subtopic = top_keywords[1] if len(top_keywords) > 1 else topic

        quote_score = 0.9 if '"' in text or "“" in text or "”" in text else 0.4

        return {
            "topic": topic,
            "subtopic": subtopic,
            "entities": entities[:8],
            "intent": intent,
            "summary_short": summary,
            "keywords": top_keywords,
            "quote_score": quote_score,
            "quality_flags": quality_flags,
        }

    def _extract_entities(self, text: str) -> list[str]:
        candidates = re.findall(r"\b([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)", text)
        seen: set[str] = set()
        output: list[str] = []
        for cand in candidates:
            key = cand.lower()
            if key not in seen:
                output.append(cand)
                seen.add(key)
        return output
