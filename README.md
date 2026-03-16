# EctoShorts

[![Hex version badge](https://img.shields.io/hexpm/v/ecto_shorts.svg)](https://hex.pm/packages/ecto_shorts)
[![Coveralls](https://github.com/MikaAK/ecto_shorts/actions/workflows/coveralls.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/coveralls.yml)
[![Credo](https://github.com/MikaAK/ecto_shorts/actions/workflows/credo.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/credo.yml)
[![Dialyzer](https://github.com/MikaAK/ecto_shorts/actions/workflows/dialyzer.yml/badge.svg)](https://github.com/MikaAK/ecto_shorts/actions/workflows/dialyzer.yml)


EctoShorts is a library built on top of Ecto that helps you handle the database operations most applications need without repeatedly writing the same query and changeset code by hand. Instead of rebuilding those patterns each time, you describe them as data and let EctoShorts handle the common cases for you. The result is less boilerplate, greater consistency across your codebase, and a faster way to implement everyday tasks such as filtering, pagination, CRUD operations, bulk writes, and transactional multi-record workflows.

EctoShorts is organized into the following components:

* `EctoShorts.Actions` — the primary entry point. It accepts a schema module and a params map, delegates to the supporting components to build and execute the query, and returns results in a consistent `{:ok, result}` / `{:error, reason}` shape.
* `EctoShorts.CommonFilters` — translates a map or keyword list of filter params into an `Ecto.Query`. Schema field keys become `WHERE` conditions, while reserved keys such as `:limit`, `:order_by`, and `:preload` become the corresponding query operations. Use this directly when you need to build a query without executing it.
* `EctoShorts.CommonChanges` — provides helpers for building `changeset/2` functions, including preloading and casting associations, trimming strings, applying conditional changes, and validating fields.
* `EctoShorts.CommonSchema` — handles schema introspection by resolving field types, association metadata, and polymorphic source information at runtime.
* `EctoShorts.CommonParams` — prepares parameter lists for `insert_all`, `update_all`, and `delete_all`, including timestamp injection and field filtering.
* `EctoShorts.CommonQuery` — inspects query structure, including named bindings, positional bindings, sources, and prefixes.
* `EctoShorts.DynamicExpressions` — builds `Ecto.Query.dynamic/2` expressions from data. The PostgreSQL adapter supports scalar comparisons, string matching, array operations, and more.
* `EctoShorts.Testing` — provides test helpers for asserting against generated SQL and dynamic expressions.

In the typical case, you call `EctoShorts.Actions` from your context module and let it handle the rest. It builds an `Ecto.Query` from `EctoShorts.CommonFilters`, can pass that query through `EctoShorts.DynamicExpressions` when you need more complex expressions, and then executes it through your configured `Ecto.Repo`.

You usually configure your repo once in `config.exs`, and EctoShorts resolves it automatically. When you need to override that behavior, every `EctoShorts.Actions` function also allows you to pass `:repo` or `:replica` at call time.

## Overview

Ecto is a powerful tool, but in practice it often makes you write more code than the task should need, or write the same kind of code over and over. In many real applications, context modules slowly fill up with almost identical query pipelines: `from`, `where`, `order_by`, `limit`, `Repo.all`, repeated for different resources with only small changes. When you add conditional filters, the repetition grows even more. Each optional parameter often leads to another `maybe_filter_*` helper that only decides whether to add a clause or leave the query as it is. The same pattern shows up in changesets, bulk operations, and transactions. None of this is very hard, but over time it adds up to a lot of boilerplate that does not add much real value.

EctoShorts solves this using data. Instead of building every query step by step, you describe what you want with a map or keyword list and pass it to `EctoShorts.Actions`. `CommonFilters` then reads that data and builds the matching `Ecto.Query` for you. Schema field keys become `WHERE` clauses, and options like `:limit`, `:order_by`, `:offset`, and `:preload` are turned into the matching query parts. More advanced comparisons, such as `%{views: %{>: 100}}`, become expressions like `WHERE views > $1`.

When a filter is more complex than simple field matching, `DynamicExpressions` takes over and builds the right `Ecto.Query.dynamic/2` expressions. This lets EctoShorts support things like string matching, array checks, and PostgreSQL-specific operators while keeping the same data-based interface. After the query is built, `Actions` runs it through your configured `Ecto.Repo` and returns a consistent `{:ok, result}` or `{:error, reason}`.

The same idea also applies to write operations. `Actions.create/3` handles the call to `Post.changeset/2` and `Repo.insert/1` for you. `Actions.create_many/3` wraps multiple inserts in an `Ecto.Multi`, so the whole transaction is rolled back if any changeset fails. `Actions.insert_all/3` uses `CommonParams` to prepare the entries by adding timestamps and removing virtual fields before calling `Repo.insert_all/2`.

The result is code that is simpler and easier to maintain. Instead of spreading query and persistence logic across many helper functions, you describe what you want as data and let `Actions` handle it in a consistent way. That keeps your code focused on application behavior instead of repetitive setup work.

## Installation

Add EctoShorts to your dependencies:

```elixir
def deps do
  [
    {:ecto_shorts, "~> 3.0"}
  ]
end
```

Configure a default repo:

```elixir
# config/config.exs
import Config

config :ecto_shorts, :repo, MyApp.Repo
```

## Configuration

All configuration lives under the `:ecto_shorts` application key.

```elixir
# config/config.exs
import Config

config :ecto_shorts,
  repo: MyApp.Repo,
  replica: MyApp.Repo.Replica,
  dynamic_adapter: EctoShorts.DynamicExpressions.Postgres,
  query_builder: MyApp.QueryBuilder,
  query_provider: MyApp.QueryProvider,
  error_module: EctoShorts.Actions.Error,
  max_positional_bindings: 10
```

Available keys:

- `:repo` - default `Ecto.Repo` used by repo-backed operations
- `:replica` - read replica repo; falls back to `:repo` when a helper supports replica fallback
- `:dynamic_adapter` - module implementing `EctoShorts.Adapter.DynamicExpression`
- `:query_builder` - module implementing `EctoShorts.Adapter.QueryBuilder`
- `:query_provider` - module implementing `EctoShorts.Adapter.QueryProvider`
- `:error_module` - module used by `EctoShorts.Actions` to build error responses
- `:max_positional_bindings` - optional override for generated positional binding support

Runtime options still take precedence over application config.

## Quick start

Assume you have:

- an `Ecto.Repo` such as `MyApp.Repo`
- a schema such as `MyApp.Blog.Post`
- a `changeset/2` function on that schema for write helpers

```elixir
alias EctoShorts.Actions
alias MyApp.Blog.Post

{:ok, post} =
  Actions.create(
    Post,
    %{title: "Hello", body: "World"},
    repo: MyApp.Repo
  )

posts =
  Actions.all(
    Post,
    %{published: true, order_by: [desc: :inserted_at], limit: 10},
    repo: MyApp.Repo
  )

{:ok, found_post} =
  Actions.find(
    Post,
    %{id: post.id},
    repo: MyApp.Repo
  )

{:ok, updated_post} =
  Actions.update(
    Post,
    post,
    %{title: "Updated"},
    repo: MyApp.Repo
  )

{:ok, _deleted_post} = Actions.delete(updated_post, repo: MyApp.Repo)
```

## `EctoShorts.Actions`

`EctoShorts.Actions` is the main public entry point.

It groups the API into five families:

- CRUD helpers such as `all/3`, `find/3`, `create/3`, `update/4`, and `delete/1-3`
- bulk helpers such as `insert_all/3`, `update_all/4`, and `delete_all/3`
- multi helpers such as `create_many/3`, `find_many/3`, `update_many/3`, and `delete_many/3`
- batch helpers such as `batch/5` and `batch_find/4`
- transaction helpers such as `transaction/2` and `transact/2`

Examples:

```elixir
alias EctoShorts.Actions
alias MyApp.Blog.Post

posts = Actions.all(Post, %{published: true}, repo: MyApp.Repo)
true = Actions.exists?(Post, %{published: true}, repo: MyApp.Repo)
{:ok, post} = Actions.find(Post, %{id: 1}, repo: MyApp.Repo)

{:ok, post} = Actions.create(Post, %{title: "Hello"}, repo: MyApp.Repo)
{:ok, post} = Actions.update(Post, post, %{title: "Updated"}, repo: MyApp.Repo)
{:ok, _post} = Actions.delete(post, repo: MyApp.Repo)
```

Bulk helpers:

```elixir
{:ok, {count, nil}} =
  Actions.insert_all(
    Post,
    [
      %{title: "Post 1", body: "Body 1"},
      %{title: "Post 2", body: "Body 2"}
    ],
    repo: MyApp.Repo
  )

{count, nil} =
  Actions.update_all(
    Post,
    %{published: false},
    %{title: "Draft"},
    repo: MyApp.Repo
  )

{count, nil} = Actions.delete_all(Post, %{published: false}, repo: MyApp.Repo)
```

Multi helpers:

```elixir
{:ok, posts} =
  Actions.create_many(
    Post,
    [
      %{title: "Post 1", body: "Body 1"},
      %{title: "Post 2", body: "Body 2"}
    ],
    repo: MyApp.Repo
  )

{:ok, posts} =
  Actions.find_many(
    Post,
    [%{id: 1}, %{id: 2}],
    repo: MyApp.Repo
  )

{:ok, posts} =
  Actions.update_many(
    Post,
    [
      {%{id: 1}, %{title: "Updated 1"}},
      {%{id: 2}, %{title: "Updated 2"}}
    ],
    repo: MyApp.Repo
  )
```

## `EctoShorts.CommonFilters`

`EctoShorts.CommonFilters.convert_params_to_filter/3` is the public query language for EctoShorts.

It accepts:

- a schema module
- a `{source, schema}` tuple
- a schemaless table name such as `"posts"`
- a prebuilt `Ecto.Query`

It always takes three arguments:

```elixir
query =
  EctoShorts.CommonFilters.convert_params_to_filter(
    MyApp.Blog.Post,
    %{
      published: true,
      views: %{>: 100},
      order_by: [desc: :inserted_at],
      limit: 10
    },
    []
  )
```

### Filter language highlights

Field keys become predicates, while reserved keys become query operations.

Examples:

```elixir
%{published: true}
%{views: %{>: 100}}
%{where: %{published: true}, or_where: %{title: "Draft"}}
%{limit: 10, offset: 20, order_by: [desc: :inserted_at]}
%{group_by: :author_id, having: %{views: %{avg: %{>: 100}}}}
%{select: [:id, :title], preload: [:author, :comments]}
```

The query filtering api includes:

- field equality and comparison operators
- boolean grouping through `:where`, `:or_where`, `:and`, and `:or`
- joins and association shorthand
- ordering, distinct, limits, offsets, `:first`, and `:last`
- projection with `:select` and `:select_merge`
- preloads, subqueries, updates, exclusions, and query prefixes
- set operations such as `:union`, `:union_all`, `:except`, and `:intersect`
- recursive CTEs, `:with_cte`, windows, and `:with_ties`
- named-binding support through top-level `:as` and `:at`

### Binding selectors

Binding selection is a top-level public API:

```elixir
%{as: %{author: %{select: :first_name}}}
%{at: %{2 => %{select: :first_name}}}
%{at: %{first: %{select: :title}}}
%{at: %{last: %{select: :inserted_at}}}
```

These shapes are first-class. They are not wrapped in a `:bind` key.

### Joins and associations

Explicit join payloads use `type:` to choose the source family and `qualifier:` to choose the join mode:

```elixir
%{join: [type: :association, source: :author, as: :author, qualifier: :left, on: true]}
```

Association shorthand is a separate surface:

```elixir
%{comments: %{approved: true}}
```

This ensures the association binding exists and applies the nested filters on that binding.

The explicit outer-key association form is also supported:

```elixir
%{join: [association: [source: :author, as: :author]]}
```

### Locks

`:lock` supports three public payload families:

- a map or keyword list with `name:`
- a raw string lock clause
- a unary function that receives the current query and returns an `Ecto.Query`

### CTEs

`:recursive_ctes` and `:with_cte` are part of the query filter public API:

```elixir
cte_query =
  from p in MyApp.Blog.Post,
    where: p.published == true

query =
  EctoShorts.CommonFilters.convert_params_to_filter(
    MyApp.Blog.Post,
    %{with_cte: %{published_posts: %{as: cte_query}}},
    []
  )
```

If clause order matters, use a keyword list rather than a map.

## `EctoShorts.CommonChanges`

`EctoShorts.CommonChanges` is for reusable changeset pipeline helpers.

Useful helpers include:

- `preload_change_assoc/3`
- `trim_string_change/2`
- `validate_not_unset/2`
- `put_new_change/3`
- `put_new_value/3`
- `apply_when/3`

Example:

```elixir
def changeset(post, attrs) do
  post
  |> cast(attrs, [:title, :slug, :published_at])
  |> EctoShorts.CommonChanges.trim_string_change([:title, :slug])
  |> EctoShorts.CommonChanges.apply_when(
    &EctoShorts.CommonChanges.changeset_field_nil?(&1, :published_at),
    &put_change(&1, :published_at, DateTime.utc_now())
  )
end
```

## `EctoShorts.CommonParams`

`EctoShorts.CommonParams` prepares payloads for repo-native bulk operations.

It is the lower-level API behind helpers such as `Actions.insert_all/3` and `Actions.update_all/4`.

Key functions include:

- `convert_to_insert_params/3`
- `build_on_conflict_options/3`
- `convert_to_update_params/3`

Example:

```elixir
{:ok, inserts} =
  EctoShorts.CommonParams.convert_to_insert_params(
    MyApp.Blog.Post,
    [
      %{title: "First post", published: true},
      %{title: "Second post", published: false}
    ]
  )
```

## `EctoShorts.CommonQuery` and `EctoShorts.CommonSchema`

Use `EctoShorts.CommonQuery` when you need to inspect bindings and sources in a query.

Use `EctoShorts.CommonSchema` when you need to normalize a source, build structs, inspect schema metadata, or create changesets from flexible source forms.

Examples:

```elixir
source = EctoShorts.CommonSchema.normalize_source({"archived_posts", MyApp.Blog.Post})
query = EctoShorts.CommonSchema.to_query(MyApp.Blog.Post)
fields = EctoShorts.CommonSchema.get_schema_reflection(MyApp.Blog.Post, :fields)
changeset = EctoShorts.CommonSchema.create_changeset(MyApp.Blog.Post, %{title: "Hello"}, [])
```

## `EctoShorts.DynamicExpressions`

`EctoShorts.DynamicExpressions.build_dynamic/4` builds `Ecto.Query.DynamicExpr` values through a dynamic adapter.

Adapter resolution order is:

1. `:dynamic_adapter` passed at call time
2. configured `EctoShorts.Config.dynamic_adapter/0`
3. auto-detection from the repo adapter

At the moment, PostgreSQL is the supported auto-resolved adapter.

```elixir
dynamic =
  EctoShorts.DynamicExpressions.build_dynamic(
    MyApp.Blog.Post,
    {:as, nil},
    {:views, [>: 1, <: 10]},
    repo: MyApp.Repo
  )
```

## `EctoShorts.Testing`

`EctoShorts.Testing` provides assertions for queries, SQL, and dynamic expressions.

Repo-bound usage:

```elixir
defmodule MyApp.PostQueryTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing, repo: MyApp.Repo

  import Ecto.Query

  alias EctoShorts.CommonFilters
  alias MyApp.Blog.Post

  test "published filter matches the expected query" do
    expected = from p in Post, where: p.published == ^true
    actual = CommonFilters.convert_params_to_filter(Post, %{published: true}, [])

    assert_query(expected, actual)
    assert_sql(expected, actual)
  end
end
```

Direct usage:

```elixir
EctoShorts.Testing.assert_sql(MyApp.Repo, expected_query, actual_query)
```

Important note: `assert_sql/4` compares the generated SQL string only. If parameter values matter for the assertion, compare full `Ecto.Adapters.SQL.to_sql/3` tuples directly.

## Common workflows

### Build a context with `Actions`

```elixir
defmodule MyApp.Blog do
  alias EctoShorts.Actions
  alias MyApp.Blog.Post

  def list_posts(params \\ %{}) do
    Actions.all(
      Post,
      Map.merge(%{order_by: [desc: :inserted_at]}, params),
      repo: MyApp.Repo
    )
  end

  def get_post(id) do
    Actions.find(Post, %{id: id}, repo: MyApp.Repo)
  end

  def create_post(attrs) do
    Actions.create(Post, attrs, repo: MyApp.Repo)
  end

  def update_post(post, attrs) do
    Actions.update(Post, post, attrs, repo: MyApp.Repo)
  end

  def delete_post(post) do
    Actions.delete(post, repo: MyApp.Repo)
  end
end
```

### Query without executing

```elixir
query =
  EctoShorts.CommonFilters.convert_params_to_filter(
    MyApp.Blog.Post,
    %{
      published: true,
      preload: [:author],
      order_by: [desc: :inserted_at]
    },
    []
  )

posts = MyApp.Repo.all(query)
```

### Work with schemaless or polymorphic sources

```elixir
EctoShorts.Actions.all("posts", %{published: true}, repo: MyApp.Repo)

EctoShorts.Actions.all(
  {"archived_posts", MyApp.Blog.Post},
  %{published: true},
  repo: MyApp.Repo
)
```

## Edge cases

- `Actions.find(source, %{}, opts)` returns `{:error, ...}` immediately for non-query sources instead of querying an arbitrary record
- `insert_all/3` and `update_all/4` execute repo-native bulk operations; for very large batches, chunking may still be appropriate
- if `with_cte`, `join`, or similar clauses rely on evaluation order, prefer keyword lists over maps

## Next steps

- Read `EctoShorts.Actions` for the main runtime API
- Read `EctoShorts.CommonFilters` for the query filter language
- Read `EctoShorts.CommonChanges` for changeset helpers
- Read `EctoShorts.Config` for runtime and application configuration
- Check `CHANGELOG.md` for release history
