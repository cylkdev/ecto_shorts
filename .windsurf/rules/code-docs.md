---
trigger: always_on
---

After implementing a feature or making a change to the codebase, use the documentation workflow to update the documentation.

## How to run

Read `.agent/DOCS.md` in full before writing any documentation. It is the single source of truth for style, structure, and formatting. Then follow the `/write-docs` workflow.

## Quick-reference checklist

These are the most commonly missed rules from `.agent/DOCS.md`. Check every one before finishing a documentation update.

**Formatting**

* Bullet lists use `*` as the prefix, never `-`.
* Options use this exact format: `* `:key` (default: `value`) - Observable effect.`
* Cross-references use backtick links: `See also `func/2` and `OtherModule`.`
* Callout boxes use admonition syntax: `> #### Title {: .warning}`
* Bold for critical emphasis: `**wrong**`, `**all**`.

**Module docs**

* Every `@moduledoc` has `##` section headers organizing its content.
* Every section contains at least one inline code example.
* Modules with 5+ public functions use `@moduledoc groups:` and `@doc group:`.
* Cross-references to related modules appear at the end.

**Function docs**

* Every public function has a `@doc` with a one-line summary, argument descriptions, exact return shapes, and examples.
* Every function has a "See also" cross-reference to related functions.
* Bang variants use "Similar to `func/arity` but raises..." instead of duplicating.
* `@doc false` is used for internal public functions (`__using__/1`, internal delegation targets).

**Types and callbacks**

* Complex public types have `@typedoc` documenting each variant.
* Callbacks document when they are invoked, every return value and its effect, and include an implementation example.