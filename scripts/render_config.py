"""Regenerate commented defaults and a complete parameter guide from typed schema."""

import json
from pathlib import Path
from typing import cast

import yaml
from pydantic import BaseModel, JsonValue

from selective_options.config import AppConfig


def render(model: BaseModel, prefix: str = "", indent: int = 0) -> tuple[list[str], list[str]]:
    lines: list[str] = []
    table: list[str] = []
    for name, field in type(model).model_fields.items():
        value = getattr(model, name)
        key = f"{prefix}.{name}" if prefix else name
        if isinstance(value, BaseModel):
            lines.append(" " * indent + name + ":")
            child, rows = render(value, key, indent + 2)
            lines.extend(child)
            table.extend(rows)
        else:
            native = list(value) if isinstance(value, tuple) else value
            scalar = (
                yaml.safe_dump(native, default_flow_style=True, width=10000)
                .strip()
                .removesuffix("...")
                .strip()
            )
            description = field.description or "See architecture for interpretation."
            lines.append(" " * indent + f"{name}: {scalar}  # {description}")
            table.append(f"| `{key}` | `{json.dumps(cast(JsonValue, native))}` | {description} |")
    return lines, table


def main() -> None:
    # model_construct avoids loading environment; nested defaults remain validated models.
    lines, table = render(AppConfig.model_construct())
    Path("config").mkdir(exist_ok=True)
    Path("config/default.yaml").write_text(
        "# Generated from config.py; no secrets. System, venue, rate_limits, ingestion and data-quality settings are operational.\n"
        + "\n".join(lines)
        + "\n",
        encoding="utf-8",
    )
    Path("PARAMETERS.md").write_text(
        "# Configuration guide\n\nSystem, venue, rate_limits, ingestion and data-quality settings are operational in the M2 pilot. Other sections are validated policy defaults reserved for later milestones. Raising a threshold never guarantees improved expectancy.\n\n| Parameter | Default | Effect |\n| --- | --- | --- |\n"
        + "\n".join(table)
        + "\n",
        encoding="utf-8",
    )
    readme = Path("README.md")
    if readme.exists():
        before, rest = readme.read_text(encoding="utf-8").split("<!-- PARAMETERS_START -->", 1)
        _, after = rest.split("<!-- PARAMETERS_END -->", 1)
        guide = "\n| Parameter | Default | Effect |\n| --- | --- | --- |\n"
        readme.write_text(
            before
            + "<!-- PARAMETERS_START -->"
            + guide
            + "\n".join(table)
            + "\n<!-- PARAMETERS_END -->"
            + after,
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
