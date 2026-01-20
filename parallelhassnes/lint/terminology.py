from __future__ import annotations

import re
from pathlib import Path


def _forbidden_phrases_from_terminology_md(terminology_md: Path) -> list[str]:
    if not terminology_md.exists():
        return []
    text = terminology_md.read_text(encoding="utf-8")
    m = re.search(r"^## Forbidden / ambiguous phrases \\(linted\\)\\s*$", text, flags=re.M)
    if not m:
        return []
    tail = text[m.end() :].splitlines()
    phrases: list[str] = []
    for line in tail:
        if line.startswith("## "):
            break
        if not line.lstrip().startswith("- "):
            continue
        for phrase in re.findall(r"`([^`]+)`", line):
            phrase = phrase.strip()
            if phrase:
                phrases.append(phrase)
    return phrases


def lint_no_synonym_drift(project_root: Path) -> list[str]:
    """
    Docs-level lint to prevent reintroducing ambiguous/overloaded terminology.

    This intentionally stays narrow: it only flags explicitly forbidden phrases
    defined in TERMINOLOGY.md, rather than attempting full semantic analysis.
    """
    terminology_md = project_root / "TERMINOLOGY.md"
    forbidden = _forbidden_phrases_from_terminology_md(terminology_md)
    if not forbidden:
        forbidden = ["run directory", "run folder", "hb age", "hb_age"]

    md_files = [
        project_root / "FUNCTIONALITY_SPEC.md",
        project_root / "Implementation Plan.md",
        project_root / "Instruction for Orchastrator.md",
    ]

    errors: list[str] = []
    for path in md_files:
        if not path.exists():
            continue
        lower = path.read_text(encoding="utf-8").lower()
        for phrase in forbidden:
            if phrase.lower() in lower:
                errors.append(f"{path.name}: contains forbidden phrase {phrase!r}")
    return errors

