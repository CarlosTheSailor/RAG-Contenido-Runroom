from __future__ import annotations

import difflib


def render_description_diff(
    current_description: str,
    proposed_description: str,
    current_source: str = "unknown",
) -> str:
    current = current_description.strip().splitlines()
    proposed = proposed_description.strip().splitlines()

    diff_lines = list(
        difflib.unified_diff(
            current,
            proposed,
            fromfile="current_description",
            tofile="proposed_description",
            lineterm="",
        )
    )

    body = "\n".join(diff_lines) if diff_lines else "No changes detected."
    current_block = current_description.strip() or "[SIN_DESCRIPCION_ACTUAL]"
    proposed_block = proposed_description.strip() or "[SIN_PROPUESTA]"

    return "\n".join(
        [
            "# Diff de descripción",
            "",
            f"Fuente descripción actual: `{current_source}`",
            "",
            "## Actual",
            "",
            "```text",
            current_block,
            "```",
            "",
            "## Propuesta",
            "",
            "```text",
            proposed_block,
            "```",
            "",
            "## Diff unificado",
            "",
            "```diff",
            body,
            "```",
            "",
        ]
    )
