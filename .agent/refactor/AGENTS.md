# Refactoring Catalog Guide

This document is the beginner-friendly guide to everything inside `.agent/refactor/`.
Use it as the authoritative map for this directory.

Important path rule:
- The real directory in this repository is `.agent/refactor/` (plural).
- If you see `.agent/refactor/` (singular) in an older document, treat that as outdated and use `.agent/refactor/` instead.

## What This Directory Is For

This directory is a reference library for improving code safely.

It contains two kinds of documents:
- `code_smells/`: documents that help you identify structural problems in code.
- `techniques/`: documents that explain named refactoring moves you can use to fix those problems.

Use the smell documents to answer:
- "What is wrong with this code shape?"
- "Why does this code keep being hard to change?"

Use the technique documents to answer:
- "What is the safest structural improvement to make next?"
- "How do I apply that improvement step by step?"

A simple rule to remember:
- A code smell names the problem.
- A refactoring technique names the fix.

## Beginner Glossary

- Module: in Elixir, a named container for functions. Example: `MyApp.Users`.
- Function: a named piece of behaviour inside a module. Example: `create_user/1`.
- Struct: a fixed-shape data structure in Elixir. It is similar to a map with a known set of fields.
- Code smell: a recurring sign that code structure is causing confusion, duplication, coupling, or change pain. A smell is not automatically a bug, but it is a reason to inspect the code carefully.
- Refactoring: changing code structure without intentionally changing the external behaviour.
- Refactoring technique: a repeatable, named way to improve structure. Example: `Extract Function`.
- Category: a folder that groups similar smells or similar techniques.
- Catalog: the full library of smell and technique documents in `.agent/refactor/`.
- Canonical path: the one path that should be treated as the source of truth. In this repo, that path is `.agent/refactor/`.

## Directory Layout

All paths in the catalog sections below are relative to `.agent/refactor/`.

Current on-disk layout:

```text
.agent/refactor/
├── AGENTS.md
├── code_smells/
│   ├── abstraction_abusers/ (4 files)
│   ├── bloaters/ (5 files)
│   ├── change_preventers/ (3 files)
│   ├── couplers/ (4 files)
│   └── dispensables/ (6 files)
└── techniques/
    ├── composing_functions/ (9 files)
    ├── dealing_with_generalization/ (12 files)
    ├── moving_features_between_modules/ (8 files)
    ├── organizing_data/ (15 files)
    ├── simplifying_conditional_expressions/ (8 files)
    └── simplifying_function_calls/ (14 files)
```

Catalog totals at the time this document was written:
- `22` code smell documents
- `66` refactoring technique documents
- `88` catalog documents in total, not counting this `AGENTS.md`

## Fast Start

Follow this order when you use the catalog:

1. Read the code you want to improve and describe the pain in one sentence.
2. Choose the closest smell category from the quick picker below.
3. Read the most likely smell file all the way through.
4. Confirm that the signs, causes, and examples match your code.
5. Choose the smallest technique that solves the root cause instead of only the visible symptom.
6. Read the chosen technique file all the way through before editing code.
7. Make small changes, keep behaviour stable, and run formatter/tests.
8. If you add, remove, move, or rename any file in `.agent/refactor/`, update this `AGENTS.md` in the same change.

Do not force a smell onto code if it does not fit.
It is normal for one change to involve:
- one primary smell
- one or two supporting smells
- one or more techniques used together

## Quick Picker

Use this section when you do not know where to start.

| If the problem sounds like this... | Start here |
| --- | --- |
| "This function or module is just too big." | `code_smells/bloaters/` |
| "One small change forces edits in many places." | `code_smells/change_preventers/` |
| "These modules know too much about each other." | `code_smells/couplers/` |
| "Some of this code probably should not exist anymore." | `code_smells/dispensables/` |
| "The abstraction is awkward, inconsistent, or fighting the design." | `code_smells/abstraction_abusers/` |
| "I need to clean up one function body." | `techniques/composing_functions/` |
| "The branching logic is the main problem." | `techniques/simplifying_conditional_expressions/` |
| "The function API is confusing or too wide." | `techniques/simplifying_function_calls/` |
| "Behaviour or data is in the wrong module." | `techniques/moving_features_between_modules/` or `techniques/organizing_data/` |
| "I need better contracts, shared logic, or variant handling." | `techniques/dealing_with_generalization/` |

## How To Read Smells And Techniques Together

Use smell files first, then technique files.

Read a smell file to learn:
- what the structural problem is
- what signs usually prove it is present
- what usually causes it
- why leaving it alone creates maintenance pain

Read a technique file to learn:
- when that technique is appropriate
- what change it makes to the code
- how to apply it in small steps
- how to validate that you did not break behaviour

Practical sequence:
1. Diagnose the code with a smell file.
2. Pick the smallest technique that addresses the root cause.
3. If one technique is not enough, combine techniques in a deliberate order.

Example:
- If you identify `LONG_FUNCTION.md`, you might next read `EXTRACT_FUNCTION.md`, `EXTRACT_VARIABLE.md`, or `REPLACE_NESTED_CONDITIONAL_WITH_GUARD_CLAUSES.md`.
- If you identify `FEATURE_ENVY.md`, you might next read `MOVE_FUNCTION.md` or `MOVE_STRUCT_FIELD.md`.

## Expected File Anatomy

This section explains what each document type normally contains so you can read and maintain the catalog consistently.

### Code Smell Files

Most smell files follow this structure:
- `# Title`
- `## Category`
- `## Description`
- `## Signs and Symptoms`
- `## Causes`
- `## Example`
- `## Refactored`
- `## Treatment`
- `## Why Refactor`

Optional sections sometimes appear:
- `## Identifying ...`
- `## Related Smells`
- `## Related Refactoring Techniques`
- category-specific notes

What this means for a beginner:
- `Description` tells you what the smell is.
- `Signs and Symptoms` tells you how to recognize it in real code.
- `Causes` tells you why it usually appears.
- `Treatment` points you toward one or more techniques that can fix it.

### Refactoring Technique Files

Most technique files follow this structure:
- `# Title`
- `## When to use`
- `## Problem`
- `## Solution`
- `## Why Refactor`
- `## How to Refactor`
- `## Validation`

Optional sections sometimes appear:
- `## Benefits`
- `## Drawbacks`
- `## Eliminates Code Smell`
- `## Similar Refactoring Techniques`
- `## Anti-Refactoring`
- short performance or safety notes

What this means for a beginner:
- `When to use` tells you whether the technique fits your case.
- `Problem` shows the structural issue the technique solves.
- `Solution` shows the shape of the improved code.
- `How to Refactor` gives you the sequence of changes.
- `Validation` tells you what must still be true after the refactor.

## Maintenance Rules

These rules keep this directory and this guide stable and easy to maintain.

### Path And Naming Rules

- Always use `.agent/refactor/` as the canonical directory path in new or updated docs.
- In this `AGENTS.md`, list file references relative to `.agent/refactor/` so paths stay short and readable.
- Keep smell files under `code_smells/<category>/`.
- Keep technique files under `techniques/<category>/`.
- Keep file names in uppercase snake case, such as `LONG_FUNCTION.md`.
- One file should document one smell or one technique only.

### Structure Rules For This AGENTS File

Keep this document in this order:
1. Purpose and glossary
2. Directory layout
3. Fast start and quick picker
4. How to read smells and techniques together
5. File anatomy
6. Maintenance rules
7. Code smell catalog
8. Technique catalog
9. Maintenance checklist

Within the catalog sections:
- keep category headings in the same order every time
- keep rows alphabetized by filename inside each category
- keep the same table columns: `Path`, `Purpose`, `Read this when`

### Update Rules When The Filesystem Changes

If any file or directory inside `.agent/refactor/` is added, removed, moved, or renamed:

1. Update the tree in `Directory Layout` if folder counts changed.
2. Update the totals in `Catalog totals` if file counts changed.
3. Update the matching row in the correct catalog table.
4. Update any other row whose summary became inaccurate because of the change.
5. Search the repository for the old path and update stale references.
6. Re-read the changed file and make sure the one-line summary in this guide still describes both:
   - what it is
   - when to use it

### Writing Rules For Beginner-Friendly Summaries

When you edit a summary row in this document:
- write for someone who does not know the term yet
- use plain language before jargon
- say what the file is for
- say when someone should read it
- avoid assuming deep Elixir knowledge

## Code Smell Catalog

These files help you diagnose the problem before choosing a fix.

### `code_smells/abstraction_abusers/` (4 files)

Use this category when the abstraction itself is part of the problem.
Typical follow-up techniques:
- `techniques/simplifying_conditional_expressions/REPLACE_CONDITIONAL_WITH_POLYMORPHISM.md`
- `techniques/dealing_with_generalization/EXTRACT_BEHAVIOUR.md`
- `techniques/organizing_data/REPLACE_TYPE_CODE_WITH_MODULE.md`
- `techniques/moving_features_between_modules/INLINE_MODULE.md`

| Path | Purpose | Read this when |
| --- | --- | --- |
| `code_smells/abstraction_abusers/ALTERNATIVE_MODULES_WITH_DIFFERENT_INTERFACES.md` | Explains the problem where modules do the same job but expose different APIs, arities, or return shapes. | Read this when callers need special handling for modules that should be interchangeable. |
| `code_smells/abstraction_abusers/REFUSED_BEQUEST.md` | Explains the problem where a module is forced to inherit, `use`, or implement behaviour it does not really want. | Read this when callbacks are stubbed, inherited functionality is mostly ignored, or a module only needs a tiny part of a larger abstraction. |
| `code_smells/abstraction_abusers/SWITCH_STATEMENTS.md` | Explains repeated branching on type, tag, or variant instead of giving each variant its own behaviour. | Read this when the same `case`, `cond`, or type check appears in several places. |
| `code_smells/abstraction_abusers/TEMPORARY_FIELD.md` | Explains structs or maps that contain fields meaningful only in some phases or code paths. | Read this when many fields are `nil` most of the time and only exist for one step of a workflow. |

### `code_smells/bloaters/` (5 files)

Use this category when code is too large, too dense, or carrying too much raw detail.
Typical follow-up techniques:
- `techniques/composing_functions/EXTRACT_FUNCTION.md`
- `techniques/moving_features_between_modules/EXTRACT_MODULE.md`
- `techniques/simplifying_function_calls/INTRODUCE_PARAMETER_STRUCT.md`
- `techniques/organizing_data/REPLACE_DATA_VALUE_WITH_STRUCT.md`

| Path | Purpose | Read this when |
| --- | --- | --- |
| `code_smells/bloaters/DATA_CLUMPS.md` | Explains groups of values that keep traveling together and should probably become their own type. | Read this when the same parameters or fields appear together again and again. |
| `code_smells/bloaters/LARGE_MODULE.md` | Explains modules that have too many lines, too many functions, or too many responsibilities. | Read this when one module is hard to describe in a single sentence or changes for many reasons. |
| `code_smells/bloaters/LONG_FUNCTION.md` | Explains functions that do too many steps inline and hide several jobs in one body. | Read this when a function has multiple step groups, deep nesting, or requires scrolling to understand. |
| `code_smells/bloaters/LONG_PARAMETER_LIST.md` | Explains function signatures that are too wide and easy to call incorrectly. | Read this when functions take many positional arguments or callers keep passing `nil`, defaults, or confusing values. |
| `code_smells/bloaters/PRIMITIVE_OBSESSION.md` | Explains overuse of raw strings, integers, atoms, and maps where domain types would be clearer. | Read this when validation and meaning are scattered because important concepts are still represented as primitives. |

### `code_smells/change_preventers/` (3 files)

Use this category when the main pain is change cost, hotspot files, or ripple effects.
Typical follow-up techniques:
- `techniques/moving_features_between_modules/EXTRACT_MODULE.md`
- `techniques/moving_features_between_modules/MOVE_FUNCTION.md`
- `techniques/moving_features_between_modules/INLINE_MODULE.md`
- `techniques/dealing_with_generalization/COLLAPSE_MODULE_HIERARCHY.md`

| Path | Purpose | Read this when |
| --- | --- | --- |
| `code_smells/change_preventers/DIVERGENT_CHANGE.md` | Explains the problem where one module changes for many unrelated reasons. | Read this when a single file is a hotspot for business rules, persistence, formatting, reporting, and other unrelated edits. |
| `code_smells/change_preventers/PARALLEL_MODULE_HIERARCHIES.md` | Explains the problem where every new concept forces matching modules in several parallel trees. | Read this when adding one feature means creating or editing the same concept across many mirrored folders. |
| `code_smells/change_preventers/SHOTGUN_SURGERY.md` | Explains the problem where one logical change requires edits in many scattered files. | Read this when you need a checklist of places to touch for one small concept change. |

### `code_smells/couplers/` (4 files)

Use this category when modules are overly dependent on each other's internal data or structure.
Typical follow-up techniques:
- `techniques/moving_features_between_modules/MOVE_FUNCTION.md`
- `techniques/moving_features_between_modules/HIDE_DELEGATE.md`
- `techniques/moving_features_between_modules/REMOVE_MIDDLE_MAN.md`
- `techniques/organizing_data/ENCAPSULATE_FIELD.md`
- `techniques/moving_features_between_modules/MOVE_STRUCT_FIELD.md`

| Path | Purpose | Read this when |
| --- | --- | --- |
| `code_smells/couplers/FEATURE_ENVY.md` | Explains functions that belong closer to another module because they mainly use that other module's data. | Read this when a function spends most of its time reaching into another struct or calling another module's helpers. |
| `code_smells/couplers/INAPPROPRIATE_INTIMACY.md` | Explains modules that know too much about each other's internals. | Read this when modules reach into private state, internal fields, or hidden implementation details. |
| `code_smells/couplers/MESSAGE_CHAINS.md` | Explains deep chains of field access or nested calls that leak object structure into callers. | Read this when callers repeatedly write long access chains like `order.customer.address.city`. |
| `code_smells/couplers/MIDDLE_MAN.md` | Explains modules that mainly forward calls without adding real policy or value. | Read this when a module is mostly pass-through delegation. |

### `code_smells/dispensables/` (6 files)

Use this category when code can be deleted, collapsed, or simplified without losing real value.
Typical follow-up techniques:
- `techniques/composing_functions/INLINE_FUNCTION.md`
- `techniques/moving_features_between_modules/INLINE_MODULE.md`
- `techniques/simplifying_function_calls/REMOVE_PARAMETER.md`
- `techniques/dealing_with_generalization/COLLAPSE_MODULE_HIERARCHY.md`
- `techniques/organizing_data/REPLACE_MAGIC_NUMBER_WITH_MODULE_ATTRIBUTE.md`

| Path | Purpose | Read this when |
| --- | --- | --- |
| `code_smells/dispensables/COMMENTS.md` | Explains comments that exist because the code is unclear instead of because domain context needs explanation. | Read this when comments describe what code does, label sections inside a function, or preserve dead code "just in case." |
| `code_smells/dispensables/DATA_MODULE.md` | Explains modules that only hold data while behaviour is scattered elsewhere. | Read this when a struct module has almost no behaviour and every real rule about that struct lives somewhere else. |
| `code_smells/dispensables/DEAD_CODE.md` | Explains unused functions, unreachable branches, commented-out logic, and orphaned modules. | Read this when code is not called anymore or exists only because nobody deleted it. |
| `code_smells/dispensables/DUPLICATE_CODE.md` | Explains repeated or near-repeated logic that should be unified. | Read this when fixes must be copied to several places or several functions differ only by small details. |
| `code_smells/dispensables/LAZY_MODULE.md` | Explains modules too small or too weak to justify their own file and boundary. | Read this when a module has one trivial function, delegates directly, or exists mainly for future hope. |
| `code_smells/dispensables/SPECULATIVE_GENERALITY.md` | Explains abstractions built for future needs that never arrived. | Read this when code has hooks, options, plugins, strategies, or parameters that no real caller actually needs. |

## Refactoring Technique Catalog

These files explain the structural moves you can make after you identify a smell.

### `techniques/composing_functions/` (9 files)

Use this category for local, function-level cleanup.
These techniques are usually the safest place to start.

| Path | Purpose | Read this when |
| --- | --- | --- |
| `techniques/composing_functions/EXTRACT_FUNCTION.md` | Shows how to move one coherent step group into a named helper function. | Read this when a function body contains several distinct jobs inline. |
| `techniques/composing_functions/EXTRACT_VARIABLE.md` | Shows how to give a dense subexpression a clear local name. | Read this when an expression is doing too much at once or the same expression repeats. |
| `techniques/composing_functions/INLINE_FUNCTION.md` | Shows how to remove a trivial helper function whose call boundary no longer adds value. | Read this when a function only wraps one obvious expression or one direct delegation. |
| `techniques/composing_functions/INLINE_TEMP.md` | Shows how to remove a one-use local binding that adds no meaning and does not cache anything important. | Read this when a variable is introduced once, used once, and only makes the code noisier. |
| `techniques/composing_functions/REMOVE_ASSIGNMENTS_TO_PARAMETERS.md` | Shows how to stop rebinding parameter names to new values and make data flow explicit. | Read this when a parameter is reused for several transformed versions inside one function. |
| `techniques/composing_functions/REPLACE_FUNCTION_WITH_MODULE.md` | Shows how to move a tangled function into a dedicated module that owns its intermediate state. | Read this when one function has too many intertwined locals for clean helper extraction. |
| `techniques/composing_functions/REPLACE_TEMP_WITH_QUERY.md` | Shows how to turn a derived local value into a named query/helper function. | Read this when the same derived value is reused or blocks cleaner extraction. |
| `techniques/composing_functions/SPLIT_TEMPORARY_VARIABLE.md` | Shows how to use different names for different intermediate values instead of reusing one binding. | Read this when one local variable means different things at different points in a function. |
| `techniques/composing_functions/SUBSTITUTE_ALGORITHM.md` | Shows how to replace a harder algorithm with a simpler equivalent implementation. | Read this when the main problem is the algorithm itself, not the function boundary. |

### `techniques/dealing_with_generalization/` (12 files)

Use this category when the codebase has shared contracts, variants, or hierarchies that need to be clarified.

| Path | Purpose | Read this when |
| --- | --- | --- |
| `techniques/dealing_with_generalization/COLLAPSE_MODULE_HIERARCHY.md` | Shows how to merge hierarchy layers that no longer provide real distinction. | Read this when a split set of parent/child modules now adds indirection without value. |
| `techniques/dealing_with_generalization/EXTRACT_BEHAVIOUR.md` | Shows how to formalize a shared contract with an Elixir behaviour. | Read this when multiple modules already act like interchangeable strategies but no explicit contract exists. |
| `techniques/dealing_with_generalization/EXTRACT_SHARED_MODULE.md` | Shows how to pull duplicated common behaviour into one shared module. | Read this when several related modules repeat the same subset of logic. |
| `techniques/dealing_with_generalization/EXTRACT_VARIANT_MODULE.md` | Shows how to pull one growing special case into its own focused variant module. | Read this when one branch or variant keeps growing inside a broader module. |
| `techniques/dealing_with_generalization/FORM_TEMPLATE_FUNCTION.md` | Shows how to share the stable steps of an algorithm while allowing a few steps to vary. | Read this when multiple modules follow the same overall workflow with only one or two differing steps. |
| `techniques/dealing_with_generalization/PULL_UP_CONSTRUCTOR_LOGIC.md` | Shows how to centralize repeated initialization logic used by several related constructors. | Read this when several `new/...` functions set the same defaults or base fields. |
| `techniques/dealing_with_generalization/PULL_UP_FUNCTION.md` | Shows how to move duplicated behaviour from sibling modules into one shared location. | Read this when the same function body appears in several related modules. |
| `techniques/dealing_with_generalization/PULL_UP_STRUCT_FIELD.md` | Shows how to centralize a field that means the same thing in several related structs. | Read this when sibling structs duplicate one shared field with the same meaning. |
| `techniques/dealing_with_generalization/PUSH_DOWN_FUNCTION.md` | Shows how to move behaviour out of a shared module and into the variants that actually need it. | Read this when a shared module owns a function relevant to only a subset of variants. |
| `techniques/dealing_with_generalization/PUSH_DOWN_STRUCT_FIELD.md` | Shows how to move fields out of a shared struct and into variant-specific structs. | Read this when a general struct contains fields irrelevant to many variants. |
| `techniques/dealing_with_generalization/REPLACE_DELEGATION_WITH_SHARED_MODULE.md` | Shows how to replace many wrapper delegates with one clear shared module boundary. | Read this when several modules mostly delegate the same calls to the same target. |
| `techniques/dealing_with_generalization/REPLACE_SHARED_MODULE_COUPLING_WITH_DELEGATION.md` | Shows how to remove forced shared-structure coupling and use explicit delegation instead. | Read this when a module is tied to a shared base mainly for reuse rather than true common identity. |

### `techniques/moving_features_between_modules/` (8 files)

Use this category when behaviour lives in the wrong module or boundaries are misplaced.

| Path | Purpose | Read this when |
| --- | --- | --- |
| `techniques/moving_features_between_modules/EXTRACT_MODULE.md` | Shows how to split one broad module into smaller modules by responsibility. | Read this when one module mixes concerns that should evolve independently. |
| `techniques/moving_features_between_modules/HIDE_DELEGATE.md` | Shows how to hide deep traversal behind a direct boundary function. | Read this when callers keep reaching through nested structs or associations to get one value. |
| `techniques/moving_features_between_modules/INLINE_MODULE.md` | Shows how to merge a weak standalone module back into its natural owner. | Read this when a tiny module has no real independent boundary and only adds navigation overhead. |
| `techniques/moving_features_between_modules/INTRODUCE_EXTERNAL_TYPE_FUNCTION.md` | Shows how to add a small local helper around a library type you do not control. | Read this when one module needs repeated app-specific behaviour for a third-party type. |
| `techniques/moving_features_between_modules/INTRODUCE_LOCAL_WRAPPER.md` | Shows how to create a stable local wrapper around an external library type or API. | Read this when app-specific helper logic for a dependency is spreading across several modules. |
| `techniques/moving_features_between_modules/MOVE_FUNCTION.md` | Shows how to move a function closer to the module whose data and rules it mainly uses. | Read this when a function seems to belong to another module's domain. |
| `techniques/moving_features_between_modules/MOVE_STRUCT_FIELD.md` | Shows how to move a field to the struct or schema that really owns it. | Read this when one field is stored in the wrong place and another module is its true owner. |
| `techniques/moving_features_between_modules/REMOVE_MIDDLE_MAN.md` | Shows how to remove pass-through wrappers so callers can use the real owner directly. | Read this when a module mostly delegates and adds no meaningful policy. |

### `techniques/organizing_data/` (15 files)

Use this category when the main issue is how data is shaped, stored, linked, or named.

| Path | Purpose | Read this when |
| --- | --- | --- |
| `techniques/organizing_data/CHANGE_BIDIRECTIONAL_ASSOCIATION_TO_UNIDIRECTIONAL.md` | Shows how to remove an unnecessary reverse relationship. | Read this when keeping both directions of an association causes more consistency work than value. |
| `techniques/organizing_data/CHANGE_REFERENCE_TO_VALUE.md` | Shows how to replace an unnecessary reference with embedded value data. | Read this when identity adds no business value and a snapshot value would be simpler or more correct. |
| `techniques/organizing_data/CHANGE_UNIDIRECTIONAL_ASSOCIATION_TO_BIDIRECTIONAL.md` | Shows how to model a relationship in both directions when both directions are truly needed. | Read this when reverse lookup logic keeps being rebuilt because only one side of the association exists. |
| `techniques/organizing_data/CHANGE_VALUE_TO_REFERENCE.md` | Shows how to replace copied value data with a shared reference to the canonical entity. | Read this when duplicated data drifts because several records should really point to one owner. |
| `techniques/organizing_data/DUPLICATE_OBSERVED_DATA.md` | Shows how to keep two representations of the same data synchronized through explicit update flow. | Read this when UI state and domain state both exist and drift because synchronization is ad hoc. |
| `techniques/organizing_data/ENCAPSULATE_COLLECTION.md` | Shows how to control collection updates through module API functions. | Read this when callers mutate lists or maps directly and bypass invariants like uniqueness or ordering. |
| `techniques/organizing_data/ENCAPSULATE_FIELD.md` | Shows how to protect a field behind explicit read/write functions. | Read this when external modules manipulate a struct field directly and you need one place for validation or normalization. |
| `techniques/organizing_data/REPLACE_DATA_VALUE_WITH_STRUCT.md` | Shows how to turn a raw primitive value into a named domain struct. | Read this when a string, integer, or map carries rules that deserve a dedicated type. |
| `techniques/organizing_data/REPLACE_MAGIC_NUMBER_WITH_MODULE_ATTRIBUTE.md` | Shows how to give important literals clear names. | Read this when raw numbers or strings hide business meaning. |
| `techniques/organizing_data/REPLACE_POSITIONAL_DATA_WITH_STRUCT.md` | Shows how to replace tuples, lists, or implicit-order data with a named struct. | Read this when code depends on positions or indexes instead of meaningful field names. |
| `techniques/organizing_data/REPLACE_SPECIALIZED_MODULES_WITH_FIELDS.md` | Shows how to collapse near-identical variant modules into one data-driven module. | Read this when several modules differ mainly by values, not by real behaviour. |
| `techniques/organizing_data/REPLACE_TYPE_CODE_WITH_MODULE.md` | Shows how to replace a type tag and repeated branching with module-based dispatch. | Read this when a type atom or string is repeatedly used to choose behaviour. |
| `techniques/organizing_data/REPLACE_TYPE_CODE_WITH_MODULE_VARIANTS.md` | Shows how to make each stable type variant its own explicit module. | Read this when a closed set of variants is growing inside one large branch-heavy module. |
| `techniques/organizing_data/REPLACE_TYPE_CODE_WITH_STATE_OR_STRATEGY.md` | Shows how to model changing runtime behaviour with state or strategy modules behind a common contract. | Read this when algorithm selection should be additive and extensible instead of handled by repeated branching. |
| `techniques/organizing_data/SELF_ENCAPSULATE_FIELD.md` | Shows how a module can route its own internal field access through helper functions. | Read this when one module accesses the same field directly in many places and that access policy may change. |

### `techniques/simplifying_conditional_expressions/` (8 files)

Use this category when branching, nesting, or flow control is harder to read than the actual work.

| Path | Purpose | Read this when |
| --- | --- | --- |
| `techniques/simplifying_conditional_expressions/CONSOLIDATE_CONDITIONAL_EXPRESSION.md` | Shows how to combine several conditions that lead to the same result. | Read this when multiple separate checks all return the same outcome. |
| `techniques/simplifying_conditional_expressions/CONSOLIDATE_DUPLICATE_CONDITIONAL_FRAGMENTS.md` | Shows how to move shared branch code out of a conditional. | Read this when both sides of an `if`, `case`, or similar branch repeat the same lines. |
| `techniques/simplifying_conditional_expressions/DECOMPOSE_CONDITIONAL.md` | Shows how to extract condition and branch logic into named functions. | Read this when one conditional is too dense to understand without comments. |
| `techniques/simplifying_conditional_expressions/INTRODUCE_ASSERTION.md` | Shows how to fail fast for impossible internal states. | Read this when a branch should never happen if the program is correct. |
| `techniques/simplifying_conditional_expressions/INTRODUCE_NULL_OBJECT.md` | Shows how to replace repeated `nil` checks with a neutral implementation that follows the same contract. | Read this when missing collaborators cause many defensive conditionals. |
| `techniques/simplifying_conditional_expressions/REMOVE_CONTROL_FLAG.md` | Shows how to replace boolean flow-control variables with direct language constructs. | Read this when flags like `found = true` exist only to manage loop or branch exit. |
| `techniques/simplifying_conditional_expressions/REPLACE_CONDITIONAL_WITH_POLYMORPHISM.md` | Shows how to move variant behaviour out of conditionals and into modules or protocols. | Read this when type-based branching appears in several places and new variants keep forcing edits. |
| `techniques/simplifying_conditional_expressions/REPLACE_NESTED_CONDITIONAL_WITH_GUARD_CLAUSES.md` | Shows how to flatten nested conditionals using early clauses or guard-style exits. | Read this when indentation and exceptional paths hide the main happy path. |

### `techniques/simplifying_function_calls/` (14 files)

Use this category when the function boundary, signature, or public API is the main source of confusion.

| Path | Purpose | Read this when |
| --- | --- | --- |
| `techniques/simplifying_function_calls/ADD_PARAMETER.md` | Shows how to make a hidden dependency explicit by adding it to the function contract. | Read this when a function secretly depends on external config or state that callers should provide. |
| `techniques/simplifying_function_calls/HIDE_FUNCTION.md` | Shows how to make internal helpers private and shrink the public API surface. | Read this when a function is public only by accident and should not be called from outside the module. |
| `techniques/simplifying_function_calls/INTRODUCE_PARAMETER_STRUCT.md` | Shows how to group related arguments into one named struct. | Read this when several parameters travel together or signatures are long and error-prone. |
| `techniques/simplifying_function_calls/PARAMETERIZE_FUNCTION.md` | Shows how to replace near-identical functions with one reusable function and a varying argument. | Read this when duplicate logic differs only by one literal or one input value. |
| `techniques/simplifying_function_calls/PRESERVE_WHOLE_STRUCT.md` | Shows how to pass one whole struct instead of passing several sibling fields separately. | Read this when callers keep unpacking the same source object to call a function. |
| `techniques/simplifying_function_calls/REMOVE_PARAMETER.md` | Shows how to remove an argument that is unused or can be derived inside the function. | Read this when a parameter adds noise but no real information. |
| `techniques/simplifying_function_calls/REMOVE_SETTER_FUNCTION.md` | Shows how to replace generic setter-style APIs with intention-revealing domain operations. | Read this when public setter functions let callers bypass invariants or create invalid states. |
| `techniques/simplifying_function_calls/RENAME_FUNCTION.md` | Shows how to rename a function so the name matches what it really does now. | Read this when callers need comments to understand a vague, stale, or misleading function name. |
| `techniques/simplifying_function_calls/REPLACE_ERROR_CODE_WITH_EXCEPTION.md` | Shows how to reserve exceptions for truly exceptional failure paths instead of generic error codes. | Read this when rare catastrophic failures clutter normal-path code with sentinel handling. |
| `techniques/simplifying_function_calls/REPLACE_EXCEPTION_WITH_TEST.md` | Shows how to replace exception-driven control flow with an explicit check. | Read this when code rescues predictable conditions that can be checked directly. |
| `techniques/simplifying_function_calls/REPLACE_NEW_WITH_FACTORY_FUNCTION.md` | Shows how to centralize construction rules in named factory functions. | Read this when callers build the same struct shape directly in many places. |
| `techniques/simplifying_function_calls/REPLACE_PARAMETER_WITH_EXPLICIT_FUNCTIONS.md` | Shows how to replace control flags or selector parameters with separate intention-revealing functions. | Read this when a boolean or type parameter selects very different behaviours. |
| `techniques/simplifying_function_calls/REPLACE_PARAMETER_WITH_FUNCTION_CALL.md` | Shows how to stop passing a derived value that the callee can compute itself. | Read this when callers repeat the same derivation before every call. |
| `techniques/simplifying_function_calls/SEPARATE_QUERY_FROM_MODIFIER.md` | Shows how to split read behaviour from write behaviour. | Read this when one function both returns information and changes state. |

## Maintenance Checklist

Use this checklist every time you maintain `.agent/refactor/` or this `AGENTS.md`.

### When You Add A New Smell File

1. Put it under exactly one folder in `code_smells/<category>/`.
2. Use uppercase snake case for the filename.
3. Follow the normal smell-file structure described above.
4. Add one row for the file in the correct code smell table in this document.
5. Update the file count for that category if needed.
6. Update the total smell-document count if needed.
7. Add or update related technique references inside the smell file if appropriate.

### When You Add A New Technique File

1. Put it under exactly one folder in `techniques/<category>/`.
2. Use uppercase snake case for the filename.
3. Follow the normal technique-file structure described above.
4. Add one row for the file in the correct technique table in this document.
5. Update the file count for that category if needed.
6. Update the total technique-document count if needed.
7. Add or update related smell references inside the technique file if appropriate.

### When You Rename Or Move A File

1. Update the path in this document.
2. Search the repository for the old path and update all stale references.
3. Re-read the file and confirm the summary row still fits.
4. If the category changed, move the row to the correct table.
5. Update category counts and totals if needed.

### When You Remove A File

1. Delete the row from the correct table in this document.
2. Update category counts and totals.
3. Search for stale references to the removed file.
4. If another file now fills the same role, update nearby summaries or cross references.

### Filesystem Verification Commands

Use these commands from the repository root to verify the directory state:

```sh
rg --files .agent/refactors | sort
find .agent/refactors -maxdepth 2 -type d | sort
```

Use these commands to find stale path references:

```sh
rg -n '\.agent/refactor/' .
rg -n '\.agent/refactor/' .
```

Use this command pattern when you rename a file and need to find old references:

```sh
rg -n 'OLD_FILE_NAME|old/path/here' .
```

### Final Review Before You Commit

Make sure all of the following are true:
- every path listed in this document exists
- category counts match the filesystem
- total counts match the filesystem
- the document still uses `.agent/refactor/` as the canonical path
- each table row explains both purpose and when to read the file
- the guide still makes sense to a beginner without needing outside context
