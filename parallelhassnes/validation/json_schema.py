from __future__ import annotations

from typing import Any


def validate_json_against_schema(schema: dict[str, Any], instance: Any) -> list[str]:
    """
    Minimal JSON Schema validator for the subset we use:
    - type, required, properties, items, enum, additionalProperties
    Returns list of error strings (empty => valid).
    """

    errors: list[str] = []

    def err(msg: str) -> None:
        errors.append(msg)

    def check(s: dict[str, Any], x: Any, path: str) -> None:
        st = s.get("type")
        if st == "object":
            if not isinstance(x, dict):
                err(f"{path}: expected object")
                return
            req = s.get("required") or []
            for k in req:
                if k not in x:
                    err(f"{path}: missing required property {k}")
            props = s.get("properties") or {}
            addl = s.get("additionalProperties", True)
            if addl is False:
                for k in x:
                    if k not in props:
                        err(f"{path}: additionalProperties not allowed: {k}")
            for k, subs in props.items():
                if k in x and isinstance(subs, dict):
                    check(subs, x[k], f"{path}.{k}")
        elif st == "array":
            if not isinstance(x, list):
                err(f"{path}: expected array")
                return
            items = s.get("items")
            if isinstance(items, dict):
                for i, it in enumerate(x):
                    check(items, it, f"{path}[{i}]")
        elif st == "string":
            if not isinstance(x, str):
                err(f"{path}: expected string")
                return
            if "enum" in s and x not in s["enum"]:
                err(f"{path}: expected one of {s['enum']}")
        elif st == "number":
            if not isinstance(x, (int, float)):
                err(f"{path}: expected number")
        elif st == "integer":
            if not isinstance(x, int):
                err(f"{path}: expected integer")
        elif st == "boolean":
            if not isinstance(x, bool):
                err(f"{path}: expected boolean")
        elif st is None:
            # allow schema without type for this MVP
            pass
        else:
            err(f"{path}: unsupported schema type {st}")

        if "enum" in s and st not in {"string"}:
            if x not in s["enum"]:
                err(f"{path}: expected one of {s['enum']}")

    if not isinstance(schema, dict):
        return ["schema is not an object"]
    check(schema, instance, "$")
    return errors

