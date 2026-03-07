# Consistent Vocabulary

Use this workflow when a concrete Markdown documentation scope should use one stable vocabulary. The goal is simple: the same concept should be named the same way everywhere, and any drift should be surfaced before edits begin.

This workflow is intentionally shard-and-collect. One coordinator owns the concept list and the final recommendations. After the scan scope is fixed, the coordinator may fan out bounded worker passes across independent file shards. Each worker pass reports vocabulary conflicts only. The coordinator then collects those reports into one deduplicated list and resolves one concept at a time with the user.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

Use `.agent/LANG.md` as the source of truth for canonical word choices, spelling, casing, and routed variants. If the inconsistency is only about wording and not meaning, resolve it in `.agent/LANG.md`.

## Input Parameters

Accept the following input parameters:

- `scope`: Required. This must state exactly what should be affected. The value must be concrete enough to resolve one file list before scanning, such as the whole project, one specific file, one specific folder, a named set of folders, or files and folders that match a stated pattern.

In the instructions below, any pattern written as `{<name>}` means the value provided for that input parameter.

## Workflow

1. Validate `{scope}` first. Do not begin until the user states exactly what should be affected.
2. If `{scope}` is missing, vague, or cannot resolve to one concrete file list, stop and require the user to narrow it. Examples of acceptable scope statements include the whole project, one file, files in one folder, or files and folders that match a specific pattern.
3. Open `.agent/LANG.md` and `.agent/DEFINITIONS.md` before you scan `{scope}` so the current canonical wording and meaning are fixed first.
4. Review only the `*.md` files resolved from `{scope}`.
5. Resolve the file list first. Do not start judging vocabulary until the scan scope is concrete.
6. Let one coordinator own the concept list and the final report order.
7. After the file list is fixed, fan out bounded worker passes across independent file shards, subdirectories, or concept families when that makes the scan faster.
8. In each worker pass, record only candidate vocabulary inconsistencies. Do not propose fixes yet.
9. For each candidate inconsistency, record all of the following:
    - the concept
    - the conflicting words or phrases
    - the exact file path
    - the exact heading, section, or line reference that shows the inconsistency
    - a short explanation of why the wording is inconsistent
10. Collect the worker-pass results into one mailbox list before you ask the user to decide anything.
11. Merge duplicates so one concept appears once, with all supporting references grouped under it.
12. Sort the inconsistency list into a stable order before you start resolution. Keep using that order until each item is resolved or explicitly deferred.
13. Work through the list one concept at a time. Do not move to the next unresolved item until the current one has a recorded decision or an explicit defer decision.

## Resolution Loop

For each inconsistency in the collected list:

1. Reason through the inconsistency before proposing a fix.
2. Propose exactly 2 solutions.
3. For each solution, record all of the following:
    - the word or phrase to standardize on
    - the places that would need to change
    - the benefits of that solution
    - the drawbacks of that solution
4. Treat each concept as a collaborative decision.
5. Present multiple choice options only. Do not ask open-ended questions.
6. Present the choices in this form:
    - `A`: adopt solution 1
    - `B`: adopt solution 2
    - `C`: keep the current wording for now and return to this item later
7. After the user chooses an option, record the decision in the mailbox list.
8. Update `.agent/LANG.md` in the same change so the canonical wording becomes the checked-in source of truth. If the decision also changes the concept meaning, update `.agent/DEFINITIONS.md` in the same change.
9. Collect the updated decision state before you move to the next unresolved concept.

## Output

The final output is a single inconsistency list that:

- names every conflicting concept found in `{scope}`
- records all supporting file references for each concept
- records the two proposed solutions for each concept
- records the user's decision for each concept before the workflow moves on
- records the required `.agent/LANG.md` update and any companion `.agent/DEFINITIONS.md` update before the workflow is considered complete

The workflow is complete only when every collected concept has one of these states:

- resolved with a chosen standard term
- deferred explicitly for later review
