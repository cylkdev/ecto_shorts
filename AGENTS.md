# EctoShorts

EctoShorts is a data-driven layer on top of Ecto for common reads, writes, bulk operations, and multi-record workflows. The core idea is simple: instead of rebuilding the same `from` / `where` / `order_by` / `Repo.*` pipelines by hand, you describe the work as maps or keyword lists and let EctoShorts build the query, changeset, or bulk payload for you.

Use this file as a fast execution manual and API map. It is not meant to replace the README or full module docs. It is meant to answer, quickly, where a behavior lives, when to use it, and where to look for live examples.

## Start Here by Task

- **Need repo-backed CRUD or query execution?** Start at `EctoShorts.Actions`.
- **Need to build a query without executing it?** Start at `EctoShorts.CommonFilters.convert_params_to_filter/3`.
- **Need reusable helpers inside a schema `changeset/2`?** Start at `EctoShorts.CommonChanges`.
- **Need `insert_all` or `update_all` payload preparation?** Start at `EctoShorts.CommonParams`.
- **Need to inspect sources, bindings, or schema metadata?** Start at `EctoShorts.CommonSchema` or `EctoShorts.CommonQuery`.
- **Need adapter-backed `dynamic/2` expressions?** Start at `EctoShorts.DynamicBuilders`.
- **Need to prove query behavior in tests?** Start at `EctoShorts.Testing`.

If you are unsure, start with `EctoShorts.Actions` for anything application-facing and drop down to the lower-level modules only when you need more control.

## Core Flow

Most read paths look like this:

1. Your code calls `EctoShorts.Actions`.
2. `EctoShorts.Actions` hands filter params to `EctoShorts.CommonFilters`.
3. `EctoShorts.CommonFilters` turns the params into an `Ecto.Query`.
4. `EctoShorts.Actions` executes the query through the configured `:repo` or `:replica`.

Most write paths look like this:

1. Your code calls a write helper in `EctoShorts.Actions`.
2. `EctoShorts.CommonSchema` builds or normalizes the schema data.
3. The schema `changeset/2` runs for normal writes, or `EctoShorts.CommonParams` prepares repo-native bulk payloads for bulk helpers.
4. The configured repo performs the insert, update, delete, multi, or transaction.

## Directory Map

- **`README.md`**: best high-level overview and cross-module examples.
- **`lib/ecto_shorts/`**: public runtime modules.
- **`lib/ecto_shorts/actions/`**: support modules behind `EctoShorts.Actions` for bulk, multi, batch, transaction, and error behavior.
- **`lib/ecto_shorts/common_filters/`**: individual filter-family builders such as joins, ordering, projection, CTEs, and locks.
- **`test/ecto_shorts/`**: main proof surface for live behavior. Start here before assuming how an API works.
- **`test/ecto_shorts/actions/`**: action-family behavior.
- **`test/ecto_shorts/common_filters/`**: filter-language behavior for schema-backed queries.
- **`test/ecto_shorts/common_filters_schemaless/`**: filter-language behavior for schemaless sources.
- **`examples/`**: runnable examples, especially `run.exs` and `ecto_query_dsl.exs`.
- **`config/`**: library config defaults and repo wiring examples.
- **`plans/`**: repo-tracked execution plans and task history.

## Configuration Knobs

The main runtime config lives under the `:ecto_shorts` application key:

- **`:repo`**: primary repo for writes.
- **`:replica`**: read repo; falls back to `:repo` when appropriate.
- **`:dynamic_adapter`**: adapter for `EctoShorts.DynamicBuilders`.
- **`:query_builder`**: custom query-builder implementation used by `EctoShorts.CommonFilters`.
- **`:query_provider`**: provider for named query expressions such as custom joins or locks.
- **`:error_module`**: custom error adapter for `EctoShorts.Actions`.
- **`:max_positional_bindings`**: cap for positional binding support.

If you need the exact behavior of repo and adapter resolution, read `lib/ecto_shorts/config.ex`.

## Public Module Map

### `EctoShorts.Actions`

This is the main public entry point for application code.

Use it when you want EctoShorts to both build and execute the work. Read helpers accept the public `CommonFilters` language. Normal write helpers go through your schema `changeset/2`. Bulk helpers use repo-native bulk operations. Multi helpers compose those operations inside `Ecto.Multi`.

Important function families:

- **CRUD**: `all/3`, `find/3`, `create/3`, `update/4`, `delete/1-3`, `exists?/3`, `preload/3`
- **Bulk**: `insert_all/3`, `update_all/4`, `delete_all/3`
- **Multi**: `create_many/3`, `find_many/3`, `update_many/3`, `delete_many/3`
- **Batch**: `batch/5`, `batch_find/4`
- **Transaction**: `transaction/2`, `transact/2`

Use `Actions` when the task is “get records”, “create or update records”, “perform this in a multi”, or “run this transactional workflow”.

Best proof surfaces:

- `lib/ecto_shorts/actions.ex`
- `test/ecto_shorts/actions/`
- `README.md`

### `EctoShorts.CommonFilters`

This is the public query language for EctoShorts. The main caller-facing entry point is `convert_params_to_filter/3`.

Use it when you need an `Ecto.Query` but do not want to execute it yet, or when you need to understand what shapes `Actions` read helpers accept.

Accepted source forms include:

- a schema module
- a `{source, schema}` tuple
- a schemaless table name like `"posts"`
- a prebuilt `Ecto.Query`

Important filter families:

- **Field predicates**: schema field keys become `WHERE` clauses.
- **Boolean groups**: `where`, `or_where`, `and`, `or`
- **Joins and associations**: `join`, association shorthand like `%{comments: %{approved: true}}`
- **Ordering and result shape**: `order_by`, `prepend_order_by`, `reverse_order`, `limit`, `offset`, `first`, `last`, `distinct`
- **Grouping and aggregation**: `group_by`, `having`, `or_having`
- **Projection**: `select`, `select_merge`
- **Loading and nesting**: `preload`, `subquery`
- **Set composition**: `union`, `union_all`, `except`, `except_all`, `intersect`, `intersect_all`
- **CTEs and windows**: `recursive_ctes`, `with_cte`, `windows`, `with_ties`
- **Bindings**: top-level `as` and `at`
- **Concurrency and utility**: `lock`, `update`, `exclude`, `put_query_prefix`, `with_named_binding`

Important usage notes:

- Use a **keyword list** instead of a map when duplicate keys or clause order matter, especially for repeated `where`, `or_where`, `join`, or `with_cte`.
- Binding selectors are **top-level public shapes**. Use `%{as: %{author: %{...}}}` or `%{at: %{2 => %{...}}}`. They are not wrapped in `:bind`.
- For joins, use association shorthand when you want “ensure this association binding exists and filter on it”. Use explicit `join:` payloads when you need source-family control, `qualifier:`, `on:`, or binding naming.
- `lock` supports three public payload families: a map or keyword list with `name:`, a raw string clause, or a unary function that receives the query and returns an `Ecto.Query`.

Best proof surfaces:

- `lib/ecto_shorts/common_filters.ex`
- `lib/ecto_shorts/common_filters/`
- `test/ecto_shorts/common_filters/`
- `test/ecto_shorts/common_filters_schemaless/`
- `examples/ecto_query_dsl.exs`

### `EctoShorts.CommonChanges`

This module is for reusable helpers inside schema `changeset/2` pipelines.

Use it when your task is about changeset ergonomics rather than query building. It is especially useful for association handling, conditional mutation, field-state checks, coercion, and defaults.

High-value functions:

- **Association workflows**: `preload_change_assoc/3`, `preload_changeset_assoc/3`, `put_or_cast_assoc/3`
- **Conditional mutation**: `apply_when/3`
- **Field-state checks**: `has_nil_change?/2`, `has_empty_change?/2`, `changeset_field_nil?/2`, `changeset_field_empty?/2`
- **Validation and coercion**: `validate_not_unset/2`, `trim_string_change/2`, `truncate_datetime_change/3`
- **Defaults**: `put_new_change/3`, `put_new_value/3`

Reach for this module when you would otherwise write repetitive changeset helpers around associations, defaults, field preservation, or cleanup.

Best proof surfaces:

- `lib/ecto_shorts/common_changes.ex`
- `test/ecto_shorts/common_changes_test.exs`

### `EctoShorts.CommonParams`

This module prepares data for repo-native bulk operations.

Use it when you want the lower-level API behind `Actions.insert_all/3` and `Actions.update_all/4`, or when you need to understand exactly how EctoShorts prepares bulk payloads.

High-value functions:

- `convert_to_insert_params/3`
- `convert_to_update_params/3`
- `build_on_conflict_options/3`

What it owns:

- validating bulk insert entries through the schema `changeset/2`
- filtering fields to query fields
- injecting timestamps
- placeholder substitution
- building `on_conflict` and `conflict_target` options
- converting update directives like `{:inc, 1}`, `{:push, value}`, and `{:pull, value}`

Use this module when the question is “what exact payload is going into `insert_all` or `update_all`?”

Best proof surfaces:

- `lib/ecto_shorts/common_params.ex`
- `test/ecto_shorts/common_params_test.exs`

### `EctoShorts.CommonSchema`

This module normalizes sources, inspects schema metadata, builds structs, and creates changesets.

Use it when you need to accept flexible source forms or you need schema metadata without hardcoding assumptions.

High-value functions:

- `normalize_source/1`
- `to_query/1`
- `get_schema/1`
- `get_schema_reflection/2-3`
- `build_struct/1`
- `create_changeset/3`

This is also the place to look when dealing with:

- schemaless sources like `"posts"`
- polymorphic `{source, schema}` tuples like `{"archived_posts", Post}`
- custom changeset resolution

Best proof surfaces:

- `lib/ecto_shorts/common_schema.ex`
- `test/ecto_shorts/common_schema_test.exs`

### `EctoShorts.CommonQuery`

This module introspects `Ecto.Query` structures at runtime.

Use it when you need to answer questions like:

- what source is this query using?
- how many bindings does it have?
- what schema does a named or positional binding point to?
- what prefix is applied?

High-value functions:

- `get_query_source/1`
- `query_binding_count/1`
- `get_query_binding_source/2`
- `get_query_prefix/1`

This is the right module when you are working on dynamic query composition, binding validation, or query inspection helpers.

Best proof surfaces:

- `lib/ecto_shorts/common_query.ex`
- `test/ecto_shorts/common_query_test.exs`

### `EctoShorts.DynamicBuilders`

This is the entry point for building `Ecto.Query.DynamicExpr` values through an adapter.

Use it when the problem is more complex than direct field filtering and you need adapter-backed dynamic expressions.

High-value function:

- `build_dynamic/4`

Adapter resolution order:

1. `:dynamic_adapter` passed at call time
2. configured `EctoShorts.Config.dynamic_adapter/0`
3. repo adapter inference

At the moment, PostgreSQL is the supported auto-resolved adapter.

Best proof surfaces:

- `lib/ecto_shorts/dynamic_builders.ex`
- `lib/ecto_shorts/dynamic_builders/`
- `test/ecto_shorts/dynamic_builders/`

### `EctoShorts.Testing`

This module provides test helpers for queries, SQL, and dynamic expressions.

Use it when you need to prove query generation behavior without inventing brittle assertions.

High-value functions:

- `assert_query/2`
- `refute_query/2`
- `assert_sql/3-4`
- `refute_sql/3-4`
- `assert_dynamic/2`
- `refute_dynamic/2`

Important note:

- `assert_sql/4` compares the generated **SQL string only**. If bound params matter, compare full `Ecto.Adapters.SQL.to_sql/3` tuples directly.

Best proof surfaces:

- `lib/ecto_shorts/testing.ex`
- `test/ecto_shorts/testing_test.exs`

## Common Scenarios

- **I need a normal filtered read.** Use `EctoShorts.Actions.all/3` or `find/3` with `CommonFilters` params.
- **I need to build a query and inspect it before execution.** Use `EctoShorts.CommonFilters.convert_params_to_filter/3`.
- **I need association filters.** Start with association shorthand if the key is a declared schema association. Use explicit `join:` when you need more control.
- **I need grouped or aggregate queries.** Use `group_by`, `having`, and related filter families in `CommonFilters`.
- **I need preloads.** Use `preload` in query params when you want eager loading on read paths. Use `CommonChanges.preload_change_assoc/3` when the work is inside a changeset pipeline.
- **I need bulk insert or bulk update behavior.** Start with `Actions.insert_all/3` or `Actions.update_all/4`. Drop to `CommonParams` when you need the exact prepared payload.
- **I need a schemaless query or alternate table with the same schema.** Use a table name string or a `{source, schema}` tuple. See `CommonSchema`.
- **I need to prove query behavior.** Start with `EctoShorts.Testing` and then read the nearest tests under `test/ecto_shorts/`.

## Where to Verify Live Behavior

Before making assumptions, check these in order:

- **`README.md`** for the fastest public overview and cross-module examples.
- **Module docs in `lib/ecto_shorts/*.ex`** for the owning boundary and accepted shapes.
- **`test/ecto_shorts/actions/`** for action behavior.
- **`test/ecto_shorts/common_filters/`** and **`test/ecto_shorts/common_filters_schemaless/`** for query-language behavior.
- **`test/ecto_shorts/common_changes_test.exs`**, **`common_params_test.exs`**, **`common_query_test.exs`**, and **`common_schema_test.exs`** for the lower-level APIs.
- **`examples/run.exs`** and **`examples/ecto_query_dsl.exs`** for runnable example flows.

Live code and tests are the primary evidence for what the library supports now.
