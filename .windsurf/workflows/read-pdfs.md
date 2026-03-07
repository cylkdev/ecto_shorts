---
description: Exports one PDF or a set of PDFs by following the source workflow in `.agent/workflows/Export PDFs.md`.
auto_execution_mode: 3
---

// turbo-all

## Source of truth

Follow `.agent/workflows/Export PDFs.md` exactly. Do not duplicate or reinterpret the PDF export workflow here.

## Input parameters

Accept these input parameters from `.agent/workflows/Export PDFs.md`:

- `target`: Required. This can be a single PDF path, a directory path, or a wildcard search pattern.
- `target_type`: Optional. Allowed values are `auto`, `file`, `directory`, and `glob`. Default to `auto` when it is not provided.
- `output_format`: Optional. Allowed values are `markdown` and `images`. Default to `markdown` when it is not provided.
- `dpi`: Optional. Use this only when `output_format` is `images`. Default to `200` when it is not provided.
- `venv_dir`: Optional. Use this only when `output_format` is `markdown`. Default to `.venv-pdf-export` when it is not provided.

## What to do

1. Read `.agent/workflows/Export PDFs.md`.

2. Execute that workflow exactly as written.
