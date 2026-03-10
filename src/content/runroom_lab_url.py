from __future__ import annotations

from urllib.parse import urlparse

from src.content.case_study_url import parse_case_study_url
from src.content.models import CanonicalDocument
from src.pipeline.normalization import slugify


def parse_runroom_lab_url(url: str) -> CanonicalDocument:
    document = parse_case_study_url(url)

    slug = document.item.slug or _slug_from_url(url) or slugify(document.item.title)

    document.item.content_key = f"runroom_lab:runroom:{slug}"
    document.item.content_type = "runroom_lab"
    document.item.slug = slug
    document.item.source = "runroom_lab_url"

    metadata = dict(document.item.metadata or {})
    metadata.update(
        {
            "content_type": "runroom_lab",
            "source": "runroom_lab_url",
            "original_url": url,
        }
    )
    document.item.metadata = metadata

    return document


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if not path:
        return ""
    return path.split("/")[-1]
