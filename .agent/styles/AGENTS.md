# Code Style Rules

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Purpose

This directory contains code style rules organized into subdirectories. Each subdirectory should have a short description that explains what area it covers and when to use its rules.

A complete beginner should be able to use this document to easily find code style rules used in this codebase.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:
* Keep this document up to date as the codebase changes.
* Keep this directory and all rule files up to date as the codebase changes.
*  When you add a new rule, place it in the correct directory and add that file to the directory layout in this document with a short description.
* When you add a new directory, add that directory to the directory layout in this document with a short description that explains what kind of rules it contains and when to use them.
* When you remove, rename, or move a rule file, update the directory layout so it still matches the current working tree.
* When you discover a repeated pattern in the codebase that should become a rule, add a rule document for it instead of relying on memory or prior discussion.
* When a rule no longer matches the codebase, revise it. Do not leave outdated rules in place.
* Before you add, revise, move, or catalog a rule, record the concrete task, key files, and required style-document updates in the surrounding active document's `Task and Key Files` section.

## Surrounding Active Document

Before you add, revise, move, or catalog any style rule, update the surrounding active document.

Record the concrete task, the key style files or directories, and every rule or catalog file that must change in that document's `Task and Key Files` section.

Keep the related document maintenance task visible in that document's `Progress` section or checklist so style-catalog sync is never treated as implicit.

## Safe Parallel Document Maintenance

When you update this document itself, let one coordinator own the final document edit. The coordinator decides the active scope, the canonical wording, and the final structure that lands in the checked-in guide.

After the change scope is stable, worker passes may inspect independent sections, companion files, or stale references in parallel. Each worker pass should return bounded facts such as outdated wording, missing sync updates, stale paths, or terminology drift.

Collect those worker-pass results before you edit this document. Do not update the guide from half-collected scans.

## Adding a New Directory

Add a new directory when you have a group of rules that belong to the same area of the system, library, or kind of work.

Create the directory using a short, clear, lowercase name. The name should describe the area the rules apply to. For example, use a name like `phoenix/`, `tests/`, or `config/`.

After you create the directory, add a short description for it in this document. The description should explain two things. It should explain what kind of rules the directory contains. It should also explain when a reader should use those rules.

Only create a new directory when the rules naturally belong together. If a rule already fits an existing directory, place it there instead of creating a new one.

Keep the structure simple. A novice reader should be able to look at the directory name and quickly understand what kind of rules it contains.

Add every rule file to the directory layout and give each file a short description that explains what rule it defines and when a reader should use it.

Treat that directory-layout update as a required task in the surrounding active document.

## Adding a New Rule File

Add a new rule file to the directory that best matches the area the rule applies to.

Use a short, clear file name that describes the rule. The name should help the reader understand what the rule is about before they open the file.

After you add the file, update the directory layout in this document. Add the file under the correct directory and include a short description that explains what rule it defines and when to use it.

If the rule does not fit any existing directory, create a new directory first, then add both the directory and the file to the directory layout.

Keep that catalog update listed in the surrounding active document until the rule file and this guide match.

## Writing Directory Descriptions

A directory description should explain two things:
  - What kind of rules the directory contains.
  - When a reader should use those rules.

Keep the description short, direct, and specific.

## Writing File Descriptions

A file description should explain two things:
  - What rule the file defines.
  - When a reader should open or apply that rule.

Keep the description short, direct, and specific.

## Directory Layout

The following is the current directory layout of the styles directory. Each directory and file should have a short description that explains what it contains and when to use it.

List every rule file in this document under the directory that contains it, and for each file include a short description that states what rule it defines and when to use that rule.

When you find a subdirectory or rule file inside this directory tree that is not already listed in this document, add it immediately under the correct subdirectory entry with its required description.

* `ecto/` - Contains coding style rules for working with the Ecto library. Use these rules when writing or reviewing Ecto queries, schemas, changesets, and other database-related code.
  * `Prefer separate steps when building an Ecto.Query.md` - Defines the rule to split incremental Ecto query construction into separate pipeline steps. Use this rule when a query is built across multiple operations instead of being expressed entirely in a single from/2 call.
