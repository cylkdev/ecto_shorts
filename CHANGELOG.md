## Changelog

### V3.0.0

**Breaking changes**

- Minimum supported Elixir version is now `~> 1.15` (was `~> 1.13`).
- `EctoShorts.CommonSchemas` has been removed. Use `EctoShorts.CommonSchema` instead.
- `EctoShorts.QueryHelpers` has been removed. Use `EctoShorts.CommonQuery` instead.
- `EctoShorts.QueryBuilder` (and its submodules) has been removed. Use `EctoShorts.Dynamics` and `EctoShorts.Compiler` instead.
- `EctoShorts.Utils.Logger` has been removed. Use `EctoShorts.Logger` instead.
- `SchemaHelpers.build_struct/2` has been removed. Use `CommonSchema.build_struct/1` instead.
- `SchemaHelpers.schema?/1` has been removed. Use `SchemaHelpers.schema_struct?/1` instead.
- `SchemaHelpers.has_schemas?/1` has been removed. Use `SchemaHelpers.any_schema_struct?/1` instead.
- `SchemaHelpers.all_schemas?/1` has been removed. Use `SchemaHelpers.all_schema_struct?/1` instead.
- `SchemaHelpers.created?/1` and `SchemaHelpers.all_created?/1` have been removed. Use `SchemaHelpers.any_created?/1` instead.
- `CommonChanges.put_when/3` has been renamed to `CommonChanges.apply_when/3`.
- `CommonFilters.create_schema_filter/3` has been replaced by `CommonFilters.create_schema_filter/6`.
- `Actions.find/3` now returns `"record not found."` instead of `"no records found"` in its error message.

**Added**

- Added `EctoShorts.CommonSchema` which provides a unified interface for working with Ecto schemas. This covers normalizing query sources, building schema structs, constructing changesets, and inspecting schema metadata.
- Added `EctoShorts.CommonQuery` which exposes utilities for inspecting `Ecto.Query` values at runtime. This lets you read the query prefix, source, binding count, and resolve individual binding sources (`get_query_prefix/1`, `get_query_source/1`, `query_binding_count/1`, `get_query_binding_source/2`).
- Added `EctoShorts.CommonParams` which provides helpers for constructing bulk operation parameters. This simplifies building conflict-handling options and converting data for insert and update operations (`build_on_conflict_options/3`, `convert_to_insert_params/3`, `convert_to_update_params/3`).
- Added `EctoShorts.Dynamics` which converts filter parameter maps into `Ecto.Query.DynamicExpr` values. Filtering behavior is customizable through a pluggable adapter.
- Added `EctoShorts.Compiler` which provides data-driven function clause generation at compile time. This adds support for `Ecto.Query` positional bindings.
- Added `EctoShorts.Logger` which provides a consistent, prefixed logging interface (`debug/2`, `info/2`, `warning/2`, `error/2`).
- Added `EctoShorts.Testing` which provides assertion helpers for verifying dynamic expressions and generated SQL. Use `use EctoShorts.Testing` to bring `assert_dynamic/2`, `refute_dynamic/2`, `assert_query/2`, `refute_query/2`, `assert_sql/3-4`, and `refute_sql/3-4` into test modules.
- Added `EctoShorts.QueryProvider` which resolves expression callbacks for joins and locks. This decouples query construction from execution.
- Added `EctoShorts.Utils.atomize_keys/1` which recursively converts string-keyed maps to atom-keyed maps using only existing atoms.
- New `Actions` helpers:
  - `preload/3` to preload associations.
  - `exists?/3` to check if a matching record exists.
  - `find_and_create/4` to find using one set of params and create using another.
  - `find_and_delete/3` to find and delete a record.
  - `transaction/2` as a thin wrapper around `Repo.transaction/2`.
  - `transact/2` for transactions that automatically roll back on `{:error, _}`.
  - `batch/5` for batch query operations by key.
  - `batch_preload/4` for batch preloading a list of entries.
- New `Config` accessors: `error_module/0`, `dynamic_adapter/0`, `query_provider/0`, `max_binding_positions/0`.
- New `SchemaHelpers` helpers:
  - `get_related_schema/2` to resolve related schemas, including `:through` associations.
  - `schema_field_type/2` to return a field's Ecto type.
  - `association_not_loaded?/2` to check if an association is `NotLoaded`.
  - `schema_module?/1` to check whether a module exports `__schema__/2`.
- New `CommonChanges` helpers:
  - `has_nil_change?/2` now accepts a field or a list of fields.
  - `has_empty_change?/2` now accepts a field or a list of fields.
  - `validate_not_unset/2` to validate fields are not unset.
  - `truncate_datetime_change/3` to truncate datetime precision.
  - `trim_string_change/2` to trim whitespace from string changes.
  - `put_new_change/3` to add a change only if it isn't already present.
  - `put_new_value/3` to set a value only when the field is currently `nil`.

**Changed**

- `Actions.stream/3` now defaults `params` to `%{}`.
- `Actions.aggregate/5` now defaults `params` to `%{}`, `aggregate` to `:count`, and `key` to `:id`.
- `Actions.find/3` now supports `:group_by` in `opts` (in addition to `:order_by`).
- `CommonFilters.convert_params_to_filter/2` now accepts an optional third `opts` argument.

### V2.4.0
- add recursive relational filtering
- add `%{field: %{!=: [1, 2, 3]}}` to allow `NOT IN ANY` queries
- fix intermittent failure from `function_exported?(schema, :create_changeset, 1)`

### V2.3.0
- add ability to optionally require a id or a cast/put assoc
- type spec fixes

### V2.2.3
- More typespec fixes

### V2.2.2
- More typespec fixes

### V2.2.1
- Use a backup error module if set to nil

### V2.2.0
- no longer require `create_changeset`, it is now optional
- Fix dialyzer issues

### V2.1.2
- fix typings a bit
- add lower/upper filters

### V2.1.1
- fix update returning wrong error format

### V2.1.0
- Make responses for Errors return as ErrorMessage

### V2.0.0
- refactor: change find_and_update to find_and_upsert and make find_and_update not do a create
- fix: make sure we can do partial updates or create with associations

### V1.1.5
- Add support for querying arrays and using filters around those
- Add ability to set `repo` option in `CommonChanges.preload_change_assoc` to set which repo to preload from

### V1.1.4
- fix change to dropping associations from find instead of taking fields so other filters pass through

### V1.1.3
- Fix relational filtering on `find_*`

### V1.1.2
- Remove relational filtering on `find_*` functions

### V1.1.2
- Add support for not equals filtering

### V1.1.1
- Fix support for order_by filtering

### V1.1.0
- Added support for querying by relation
- Remove order_by from `convert_params_to_filter` arguments and implement as a parameter

### V1.0.0
- Multi-repo & Replica support
- Add `find_and_update`
- Add `find_or_create_many`
- Add `stream`
- Passing nil as a param results in an error (BREAKING)
- find now returns an `Ecto.MultipleResultsError` if more than one result is being returned from the query (BREAKING)

### V0.1.5
- Add `find_or_create` for Actions

### V0.1.4
- Update schema
- Add better specs
- Bug fixes for update

### V0.1.3
- Add `like` filter param
- Add more documentation

### V0.1.2
- Add more documentation

### V0.1.1
- Add some documentation fixes

### V0.1.0
- Initial Release
