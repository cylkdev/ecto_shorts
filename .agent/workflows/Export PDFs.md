# Export PDFs

Treat the reader as a complete beginner to this repository: they have only the current working tree and this workflow. There is no memory of prior work and no external context.

Use this workflow when one PDF or a set of PDFs in the working tree must be turned into something easier to read. The workflow supports two outputs. The default output format is `markdown`, which writes a sibling `.md` file beside each PDF. The alternate output format is `images`, which writes one PNG file per page into a sibling `*_pages` folder beside each PDF.

The target is flexible. It can be one PDF file, one directory, or one wildcard search. The workflow always resolves that target into a concrete list of PDFs first. It then exports each resolved PDF one at a time. Choose `markdown` when readable text is the main goal. Choose `images` when the appearance of the original page matters more than text extraction quality. The workflow is complete only when the selected sibling outputs exist and are readable enough for the next task.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Input Parameters

Accept the following input parameters:

- `target`: Required. This can be a single PDF path, a directory path, or a wildcard search pattern.
- `target_type`: Optional. Allowed values are `auto`, `file`, `directory`, and `glob`. If it is not provided, use `auto`.
- `output_format`: Optional. Allowed values are `markdown` and `images`. If it is not provided, use `markdown`.
- `dpi`: Optional. Use this only when `output_format` is `images`. If it is not provided, use `200`.
- `venv_dir`: Optional. Use this only when `output_format` is `markdown`. If it is not provided, use `.venv-pdf-export`.

In the instructions below, any pattern written as `{<name>}` means the value provided for that input parameter. If an optional parameter was not provided, replace it with the default value defined in this section before running the commands.

## Before You Start

This workflow is macOS-only. Use `zsh` or `bash` for the commands below.

A `sibling output` is an output file or folder created in the same directory as the source PDF. If a resolved PDF is `.docs/specs/Query Guide.pdf`, the sibling Markdown output is `.docs/specs/Query Guide.md`, and the sibling image output folder is `.docs/specs/Query Guide_pages/`. Keep the output beside each PDF so the location stays predictable and easy to verify.

The Markdown path uses `MarkItDown`, which converts supported documents into Markdown. The image path uses `pdftoppm`, which renders each PDF page into a PNG file. A `virtual environment` is an isolated Python directory that keeps Python package installation for this workflow separate from other projects on the machine.

A file target looks like `.docs/specs/Query Guide.pdf`. A directory target looks like `.docs/specs`. A wildcard target looks like `.docs/**/*.pdf` or `*.pdf`. When the target resolves to multiple PDFs, this workflow processes every matching PDF and writes one sibling output per file.

If `brew` does not exist on the machine and you need to install dependencies, install Homebrew first:

```sh
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

If the Homebrew installer prints shell setup commands, run those exact commands before continuing.

## Parallel Export Model

One coordinator owns the resolved PDF list, the chosen `output_format`, and the shared setup values such as `dpi` or `venv_dir`. Fix those inputs first.

After the inputs are stable, worker passes may export different PDFs in parallel because each source PDF writes to its own sibling output path. Each worker pass should report the source PDF, the output path it wrote, and any failure that blocked that export.

Collect all worker-pass results before validation. Keep virtual-environment setup, dependency installation, and any rerun decision under one coordinator.

## Resolve the Target PDFs

Start from the repository root when you use a wildcard target. Wildcard targets are resolved relative to the current directory.

Set up the working variables first:

```sh
target="{target}"
target_type="{target_type}"
output_format="{output_format}"
dpi="{dpi}"
venv_dir="{venv_dir}"
pdf_list_file="$(mktemp)"
```

If you are using defaults, replace the optional values like this before you continue:

```sh
[ -n "$target_type" ] || target_type="auto"
[ -n "$output_format" ] || output_format="markdown"
[ -n "$dpi" ] || dpi="200"
[ -n "$venv_dir" ] || venv_dir=".venv-pdf-export"
```

If `target_type` is `auto`, detect the target type from the working tree:

```sh
if [ "$target_type" = "auto" ]; then
  if [ -f "$target" ]; then
    target_type="file"
  elif [ -d "$target" ]; then
    target_type="directory"
  else
    target_type="glob"
  fi
fi
```

If `target_type` is anything other than `file`, `directory`, or `glob` after that detection step, stop and correct it before continuing.

Resolve the target into a concrete list of PDFs:

```sh
case "$target_type" in
  file)
    case "$target" in
      *.pdf|*.PDF) printf '%s\n' "$target" > "$pdf_list_file" ;;
      *) echo "Target file is not a PDF: $target" >&2; exit 1 ;;
    esac
    ;;
  directory)
    find "$target" -type f \( -iname '*.pdf' \) | sort > "$pdf_list_file"
    ;;
  glob)
    if command -v rg >/dev/null 2>&1; then
      rg --files -g "$target" | awk '/\.[Pp][Dd][Ff]$/' > "$pdf_list_file"
    else
      echo "Wildcard searches require rg. Install rg or use a file or directory target instead." >&2
      exit 1
    fi
    ;;
  *)
    echo "Unsupported target_type: $target_type" >&2
    exit 1
    ;;
esac
```

Stop immediately if no PDFs were resolved:

```sh
if [ ! -s "$pdf_list_file" ]; then
  echo "No PDFs matched target: $target" >&2
  exit 1
fi
```

Inspect the resolved list before you export anything:

```sh
resolved_count="$(wc -l < "$pdf_list_file" | tr -d ' ')"
echo "Resolved $resolved_count PDF(s):"
sed -n '1,20p' "$pdf_list_file"
```

If the resolved list is not the set of PDFs you intended to export, stop and correct `target` or `target_type` before continuing.

If `output_format` is anything other than `markdown` or `images`, stop and correct it before continuing.

## Export to Markdown

Use this path when `output_format` is `markdown`. This path writes a sibling `.md` file beside each resolved PDF. It depends on `python3`, a Python version of `3.10` or newer, and `markitdown[pdf]` installed into a virtual environment.

Confirm that `python3` exists:

```sh
command -v python3
```

If `python3` is missing, install Python:

```sh
brew install python
```

Confirm that the Python version is `3.10` or newer:

```sh
python3 --version
```

If the version is older than `3.10`, stop and install a newer Python before continuing.

Create the virtual environment if it does not already exist:

```sh
[ -d "$venv_dir" ] || python3 -m venv "$venv_dir"
```

Activate the virtual environment:

```sh
source "$venv_dir/bin/activate"
```

Install MarkItDown with PDF support:

```sh
python3 -m pip install --upgrade pip
python3 -m pip install 'markitdown[pdf]'
```

Export every resolved PDF to a sibling Markdown file:

```sh
while IFS= read -r pdf; do
  markitdown "$pdf" -o "$(dirname "$pdf")/$(basename "$pdf" .pdf).md"
done < "$pdf_list_file"
```

The first Markdown export may take longer than later runs because Python packages must be installed before the conversion can run.

## Export to Images

Use this path when `output_format` is `images`. This path writes a sibling `*_pages` folder beside each resolved PDF. It depends on `pdftoppm`, which is commonly installed on macOS by installing `poppler`.

Confirm that `pdftoppm` exists:

```sh
command -v pdftoppm
```

If `pdftoppm` is missing, install `poppler`:

```sh
brew install poppler
```

Export every resolved PDF to a sibling image folder:

```sh
while IFS= read -r pdf; do
  outdir="$(dirname "$pdf")/$(basename "$pdf" .pdf)_pages"
  mkdir -p "$outdir"
  pdftoppm -png -r "$dpi" "$pdf" "$outdir/page"
done < "$pdf_list_file"
```

## Validation and Acceptance

Validate immediately after the export command finishes.

If `output_format` is `markdown`, first confirm that every resolved PDF produced a sibling `.md` file:

```sh
while IFS= read -r pdf; do
  md_file="$(dirname "$pdf")/$(basename "$pdf" .pdf).md"
  [ -f "$md_file" ] || { echo "Missing Markdown output for: $pdf" >&2; exit 1; }
done < "$pdf_list_file"
```

Then inspect one representative output file:

```sh
first_pdf="$(head -n 1 "$pdf_list_file")"
sed -n '1,40p' "$(dirname "$first_pdf")/$(basename "$first_pdf" .pdf).md"
```

The Markdown path is successful only when every resolved PDF has a sibling `.md` file and the sampled content is readable enough for the next task.

If `output_format` is `images`, first confirm that every resolved PDF produced at least one PNG file in its sibling image folder:

```sh
while IFS= read -r pdf; do
  outdir="$(dirname "$pdf")/$(basename "$pdf" .pdf)_pages"
  ls "$outdir"/*.png >/dev/null 2>&1 || { echo "Missing image output for: $pdf" >&2; exit 1; }
done < "$pdf_list_file"
```

Then inspect one representative output folder:

```sh
first_pdf="$(head -n 1 "$pdf_list_file")"
ls "$(dirname "$first_pdf")/$(basename "$first_pdf" .pdf)_pages"
```

The image path is successful only when every resolved PDF has a sibling `*_pages` folder with one or more `page-*.png` files and the sampled images are readable.

## Recovery and Restart

This workflow is safe to restart. The source of truth is always the combination of `target`, `target_type`, `output_format`, and the resolved PDF list. Treat every rerun as a recovery step. Keep the target the same unless you have proved that the original target was wrong.

If no PDFs were resolved, correct `target` or `target_type` and rerun the target-resolution steps before you export anything.

If a wildcard target produced the wrong set of PDFs, rerun the workflow from the repository root and inspect the resolved list before exporting. Wildcard searches are only safe when the resolved list matches your intent.

If `markitdown` is not found after installation, the virtual environment is probably not active in the current shell. Reactivate it and rerun the Markdown export:

```sh
source "$venv_dir/bin/activate"
while IFS= read -r pdf; do
  markitdown "$pdf" -o "$(dirname "$pdf")/$(basename "$pdf" .pdf).md"
done < "$pdf_list_file"
```

If the Markdown output is too weak, too sparse, or too hard to read confidently, switch to `images` and follow the image path instead. That is the correct fallback when text extraction quality is worse than the visual readability of the original PDF.

If `pdftoppm` is missing, install `poppler` and rerun the image export:

```sh
brew install poppler
while IFS= read -r pdf; do
  outdir="$(dirname "$pdf")/$(basename "$pdf" .pdf)_pages"
  mkdir -p "$outdir"
  pdftoppm -png -r "$dpi" "$pdf" "$outdir/page"
done < "$pdf_list_file"
```

If the page images are blurry, increase `dpi` to `300` and rerun the image export.

If a sibling image folder already contains old `page-*.png` files for a resolved PDF and you need a clean rerun, inspect that folder first and remove only the generated page images for that PDF:

```sh
while IFS= read -r pdf; do
  outdir="$(dirname "$pdf")/$(basename "$pdf" .pdf)_pages"
  ls "$outdir"
done < "$pdf_list_file"
```

Only if each listed folder is clearly the correct sibling folder for its PDF, remove the old generated images and rerun the image export:

```sh
while IFS= read -r pdf; do
  outdir="$(dirname "$pdf")/$(basename "$pdf" .pdf)_pages"
  rm "$outdir"/page-*.png
  pdftoppm -png -r "$dpi" "$pdf" "$outdir/page"
done < "$pdf_list_file"
```
