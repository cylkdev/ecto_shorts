## Changelog

### V2.5.0
- Enhanced `EctoShorts.CommonChanges` module with new validation and transformation helpers
  - Added `validate_not_unset/2` and `validate_not_unset/3`: Prevent fields from being set to nil after they've been persisted
  - Added `truncate_datetime_change/2` and `truncate_datetime_change/3`: Truncate DateTime/NaiveDateTime fields to a specified precision
  - Added `trim_string_change/2`: Trim whitespace from string fields
  - Added `put_new_change/2` and `put_new_change/3`: Set a change only if the field hasn't already been changed
  - Added `put_new_value/2` and `put_new_value/3`: Set a change only if the field is currently nil
  - Renamed `put_when/3` to `apply_when/3`: Conditionally apply a changeset function based on a predicate

- Added `EctoShorts.CommonSchema` module for working with Ecto schemas and polymorphic associations
  - `get_schema_reflection/2` and `get_schema_reflection/3`: Query schema metadata (fields, types, etc.) from a schema module, schema struct, or polymorphic tuple
  - `get_schema_prefix/1`: Extract the database schema prefix from a schema
  - `get_schema_source/1`: Get the database table name and schema module as a tuple
  - `get_schema_metadata/1`: Extract the `Ecto.Schema.Metadata` struct from a schema or changeset
  - `put_schema_metadata/2`: Update schema metadata (state, source, prefix, context)
  - `to_schema_struct/1`: Convert a schema module or polymorphic tuple into a schema struct
  - `create_changeset/3` and `create_changeset/4`: Build a changeset from various input types (schema, struct, params, or changeset) with optional custom changeset callback

- Added `EctoShorts.CommonQuery` module for introspecting `Ecto.Query` structures
  - `get_query_source/1`: Extract the root source tuple `{table_name, schema}` from a query, traversing through composed queries and subqueries
  - `get_query_prefix/1`: Get the database schema prefix from a query
  - `query_binding_count/1`: Count the total number of bindings (from + joins) in a query
  - `get_query_binding_source/2`: Resolve a specific binding by name (`:as` alias) or position, with support for association joins

- Added `EctoShorts.CommonParams` module for preparing bulk insert and update operations
  - `convert_to_insert_params/3`: Transform a list of maps or structs into the format required by `Ecto.Repo.insert_all/3`, with support for validation, automatic timestamp generation, placeholder substitution, and conflict resolution configuration
  - `convert_to_update_params/3`: Convert update parameters into the format required by `Ecto.Repo.update_all/3`, with support for update operations (`:set`, `:inc`, `:push`, `:pull`) and automatic `updated_at` timestamp management
  - Handles optional schema validation through changesets, customizable timestamp field names and types, and placeholder value substitution for deferred resolution

- Changed `EctoShorts.QueryBuilder` module interface (**BREAKING**)
  - Replaced `create_schema_filter/4` callback with `build_query/6` callback for more flexible query building
  - The new `build_query/7` function accepts a binding selector (`{:at, position}` or `{:as, name}`) and additional options for advanced query construction
  - This change allows adapters to build more complex queries with explicit binding control

- Overhauled `EctoShorts.CommonFilters.convert_params_to_filter/2` filter conversion pipeline
  - Added support for binding-based filtering via `:as` (named bindings) and `:at` (positional bindings) parameters
  - Improved filter parameter normalization to handle nested data structures
  - Added support for multi-level filter nesting (e.g., `%{field: {operator: {nested_operator: value}}}`)
  - Enhanced association filtering with automatic join detection and application
  - Added support for select/select_merge operations with flexible value formats (boolean, list, atom, map)
  - Improved error handling and logging when composing queries
  - Added support for boolean operators (`:and`, `:or`) in filter chains
  - **Note**: This refactor may change behavior in edge cases; review existing filter usage

- Added `EctoShorts.QueryBuilder` module as the entry point for database-specific query building adapters
  - `build_query/6`: Routes query building requests to the appropriate adapter (Postgres by default, or custom via `:query_builder` option)
  - Supports pluggable adapters for extending query building behavior across different database systems

- Added `EctoShorts.QueryBuilder.Postgres` adapter implementing the `QueryBuilder` behavior for PostgreSQL

#### V2.4.0
- add recursive relational filtering
- add `%{field: %{!=: [1, 2, 3]}}` to allow `NOT IN ANY` queries
- fix intermittent failure from `function_exported?(schema, :create_changeset, 1)`

#### V2.3.0
- add ability to optionally require a id or a cast/put assoc
- type spec fixes

#### V2.2.3
- More typespec fixes

#### V2.2.2
- More typespec fixes

#### V2.2.1
- Use a backup error module if set to nil

#### V2.2.0
- no longer require `create_changeset`, it is now optional
- Fix dialyzer issues

#### V2.1.2
- fix typings a bit
- add lower/upper filters

#### V2.1.1
- fix update returning wrong error format

#### V2.1.0
- Make responses for Errors return as ErrorMessage

#### V2.0.0
- refactor: change find_and_update to find_and_upsert and make find_and_update not do a create
- fix: make sure we can do partial updates or create with associations

#### V1.1.5
- Add support for querying arrays and using filters around those
- Add ability to set `repo` option in `CommonChanges.preload_change_assoc` to set which repo to preload from

#### V1.1.4
- fix change to dropping associations from find instead of taking fields so other filters pass through

#### V1.1.3
- Fix relational filtering on `find_*`

#### V1.1.2
- Remove relational filtering on `find_*` functions

#### V1.1.2
- Add support for not equals filtering

#### V1.1.1
- Fix support for order_by filtering

#### V1.1.0
- Added support for querying by relation
- Remove order_by from `convert_params_to_filter` arguments and implement as a parameter

#### V1.0.0
- Multi-repo & Replica support
- Add `find_and_update`
- Add `find_or_create_many`
- Add `stream`
- Passing nil as a param results in an error (BREAKING)
- find now returns an `Ecto.MultipleResultsError` if more than one result is being returned from the query (BREAKING)

#### V0.1.5
- Add `find_or_create` for Actions

#### V0.1.4
- Update schema
- Add better specs
- Bug fixes for update

#### V0.1.3
- Add `like` filter param
- Add more documentation

#### V0.1.2
- Add more documentation

#### V0.1.1
- Add some documentation fixes

#### V0.1.0
- Initial Release
