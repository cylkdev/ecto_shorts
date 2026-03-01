defmodule EctoShorts.CommonFilters do
  @moduledoc since: "3.0.0"
  @moduledoc """
  EctoShorts.CommonFilters provides a data-driven api for building queries.

  ## Getting started

  The main function you will use is `convert_params_to_filter/3`. This function
  takes a map or keyword list of params that describe the actions we want to take
  and converts them into an `Ecto.Query`.

  The first argument is called the `source` which is just a fancy term for the
  table we want to operate on. It can be a schema module, a `{source, schema}`
  tuple, or an existing `Ecto.Query`.

  The second argument can be a one or many map or keyword-lists of params that
  describe the actions we want to take.

  For example:

      EctoShorts.CommonFilters.convert_params_to_filter(Post, %{published: true, limit: 10})
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.published == ^true, limit: ^10>

  The parameters are an intuitive filtering language which you can write as
  sentences.

  For example:

  As a list:

      I want only records where `:published` is true.
      I want at most 10 results.

  As a single sentence:

      "I want only records where `:published` is `true` and I want at most 10 results."

  As actions we would take if we wanted to build by hand:

      Add a `where` condition so only records with `published == true` match.
      Use `Ecto.Query.limit/2` so the query returns at most 10 records.

  All of these encode the same intent which is to produce the SQL:

      SELECT *
      FROM posts AS p0
      WHERE p0.published = TRUE
      LIMIT 10;

  Maps and keyword-lists can be used interchangeably. Keyword-lists gives us
  control over the order of operations and is more flexible for complex queries.

  ## Filters

  A filter is an `Ecto.Query` operation we can apply (e.g. `Ecto.Query.where/2`).
  A filter key is a part of the syntax which tells `convert_params_to_filter/3`
  which filter to apply.

  ### Schema Filters

  To filter by keys that exist on the given Ecto.Schema, simply provide the key
  and value as a map or keyword list:

      %{title: "hello"}
      %{published: true}
      %{id: 1}

  By default each key-value pair is treated as a schema filter which are mapped
  to Ecto.Query where clauses.

  For example, you can select all records with nil values:

      %{published_at: nil}

  or you can select all records where the value is the member of a list:

      %{published: [true, false]}

  Schema filters are typically type-aware unless the dynamic expression adapter
  does not support it or you are performing a schema-less operation. In other
  words, the behaviour changes based on the type of the field.

  For example if the field is a type of array, given the paramters:

      %{published: [true, false]}

  the resulting expression is an equality check:

      p.published == ^[true, false]

  and if the field is a scalar type, the same parameter would be treated as a
  membership check:

      p.published in ^[true, false]

  ### Comparison operators

  Use an operator map when you want a specific comparison instead of the default
  behaviour.

  The operator is the map key, and the operand is the map value:

      %{views: %{>: 10}}       # WHERE views > 10
      %{views: %{>=: 10}}      # WHERE views >= 10
      %{views: %{<: 10}}       # WHERE views < 10
      %{views: %{<=: 10}}      # WHERE views <= 10
      %{id: %{==: 1}}          # WHERE id = 1
      %{views: %{!=: 10}}      # WHERE views != 10

  Supported comparison operators:

    * `:==` / `:eq`  - equal
    * `:!=` / `:ne`  - not equal
    * `:> ` / `:gt`  - greater than
    * `:>=` / `:gte` - greater than or equal
    * `:< ` / `:lt`  - less than
    * `:<=` / `:lte` - less than or equal

  When `:==` or `:!=` is set to a list, it becomes `IN` or `NOT IN` (this forces
  membership semantics even in cases where the default schema filter behaviour
  might differ based on the field type):

      %{published: %{==: [true, false]}}   # WHERE published IN (true, false)
      %{published: %{!=: [true, false]}}   # WHERE published NOT IN (true, false)

  When `:==` or `:!=` is set to `nil`, it becomes `IS NULL` or `IS NOT NULL`:

      %{published_at: %{==: nil}}          # WHERE published_at IS NULL
      %{published_at: %{!=: nil}}          # WHERE published_at IS NOT NULL

  Use `:in` when you want to express membership explicitly:

      %{published: %{in: [true, false]}}   # WHERE published IN (true, false)
      %{id: %{in: [1, 2, 3]}}              # WHERE id IN (1, 2, 3)

  ### Negation

  Conditions that appear under the `:not` key are flipped (negated).
  If the condition would be true, `:not` makes it false. If the condition would
  be false, `:not` makes it true.

  When the key `:not` appears after the `field` and before an operator or value,
  it negates the expression inside of it:

      %{views: %{not: %{>: 10}}}                 # WHERE NOT (views > 10)
      %{views: %{not: %{>=: 10}}}                # WHERE NOT (views >= 10)
      %{views: %{not: %{==: 10}}}                # WHERE views != 10
      %{published: %{not: %{in: [true, false]}}} # WHERE published NOT IN (...)
      %{views: %{not: %{avg: %{>: 10}}}}         # HAVING NOT (avg(views) > 10)

  ### String matching

  The `:like` and `:ilike` operators perform pattern matching.
  Patterns are automatically wrapped with `%` wildcards:

      %{title: %{like: "hello"}}       # WHERE title LIKE '%hello%'
      %{title: %{ilike: "hello"}}      # WHERE title ILIKE '%hello%'

  Pass a list of patterns to match any of them:

      %{title: %{like: ["hello", "world"]}}    # WHERE title LIKE ANY(...)

  Negate with `:not`:

      %{title: %{not: %{like: "hello"}}}    # WHERE NOT (title LIKE '%hello%')

  ### String transformations

  The `:lower` and `:upper` keys transform the field value before
  comparing:

      %{title: %{==: %{lower: "hello"}}}    # WHERE lower(title) = 'hello'
      %{title: %{!=: %{upper: "HELLO"}}}    # WHERE upper(title) != 'HELLO'

  ## Logical operators

  ### Field-level `:and` and `:or`

  Combine multiple conditions on the same field with `:and` or `:or`:

      %{views: %{and: [>: 10, <: 20]}}             # WHERE views > 10 AND views < 20
      %{views: %{or: [>: 10, <: 5]}}               # WHERE views > 10 OR views < 5
      %{published: %{or: [==: true, ==: false]}}    # WHERE published = true OR published = false

  An empty list is a no-op — the query is returned unchanged.

  ### Top-level `:and` and `:or`

  At the top level, `:and` and `:or` combine complete filter conditions.
  They wrap a list of keyword lists or maps:

      %{or: [[published: true, views: 20], [published: false, views: 10]]}
      # WHERE (published = true AND views = 20) OR (published = false AND views = 10)

      %{and: [[published: true, views: 20], [title: "hello", views: 15]]}
      # WHERE (published = true AND views = 20) AND (title = 'hello' AND views = 15)

  You can mix top-level `:or` with regular field filters:

      [title: "test", or: [[published: true, views: 20], [published: false, views: 10]]]
      # WHERE title = 'test' AND ((published AND views = 20) OR (NOT published AND views = 10))

  ### The `:where` and `:or_where` keys

  Use `:where` and `:or_where` for explicit control over how conditions
  are grouped. The system always processes `:where` before `:or_where`,
  regardless of map key order. Bare field filters behave as implicit
  `:where` filters, so these two are equivalent:

      %{published: true, or_where: %{published: false}}
      %{where: %{published: true}, or_where: %{published: false}}
      # Both produce: WHERE published = true OR published = false

  Explicit `:where` accepts a map or keyword list of filters:

      %{where: %{published: true, views: 10}}
      %{where: [published: true, views: 10]}

  ## Aggregate functions

  Aggregate keys (`:avg`, `:count`, `:sum`, `:max`, `:min`) wrap a
  comparison operator. They are typically used inside a `:having`
  clause:

      %{group_by: :author_id, having: %{views: %{avg: %{>: 10}}}}
      %{group_by: :author_id, having: %{views: %{count: %{>: 10}}}}

  When an aggregate receives a plain value, it defaults to equality:

      %{views: %{avg: 10}}   # same as %{views: %{avg: %{==: 10}}}

  Negate an aggregate by wrapping it with `:not`:

      %{views: %{not: %{avg: %{>: 10}}}}   # HAVING NOT (avg(views) > 10)

  ## Arithmetic expressions

  Arithmetic operators (`:+`, `:-`, `:*`, `:/`) take a two-element
  list and appear inside comparison operators:

      %{views: %{>: %{+: [:views, 10]}}}    # WHERE views > views + 10
      %{views: %{>: %{*: [:views, 2]}}}     # WHERE views > views * 2

  ## Date and time expressions

  Date/time type wrappers (`:datetime`, `:date`) wrap an operation
  (`:add`, `:ago`, `:from_now`) inside a comparison:

      %{inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}}
      # WHERE inserted_at >= datetime_add(inserted_at, 1, 'day')

      %{inserted_at: %{>: %{datetime: %{ago: %{count: 1, interval: "day"}}}}}
      # WHERE inserted_at > (now - 1 day)

      %{inserted_at: %{>: %{datetime: %{from_now: %{count: 1, interval: "day"}}}}}
      # WHERE inserted_at > (now + 1 day)

  ## Subquery and set comparisons

  The `:all` and `:any` keys appear inside a comparison operator and
  wrap a subquery expression:

      subquery_expr = from(c in "comments", select: c.post_id)

      %{id: %{>: %{all: subquery_expr}}}    # WHERE id > ALL(subquery)
      %{id: %{>: %{any: subquery_expr}}}    # WHERE id > ANY(subquery)

  When no comparison operator is provided, they default to equality:

      %{id: %{all: subquery_expr}}    # WHERE id = ALL(subquery)

  You can pass a query-builder payload instead of a pre-built subquery:

      %{id: %{>: %{all: [source: Post, query: %{id: 1}]}}}

  ## Array fields

  Array fields (like `{:array, :string}`) support special operators
  for array membership and equality:

      %{tags: "elixir"}                    # WHERE 'elixir' = ANY(tags)
      %{tags: %{in: ["elixir"]}}           # WHERE tags @> ARRAY['elixir']
      %{tags: %{==: ["elixir", "erlang"]}} # WHERE tags = ARRAY['elixir', 'erlang']
      %{tags: %{!=: ["elixir"]}}           # WHERE tags != ARRAY['elixir']

  Use `:all` with `:in` to require all values:

      %{tags: %{all: %{in: ["elixir", "erlang"]}}}   # WHERE tags @> ARRAY['elixir', 'erlang']

  Negate with `:not`:

      %{tags: %{not: %{in: ["elixir"]}}}
      %{tags: %{not: %{all: %{in: ["elixir", "erlang"]}}}}

  Array fields also support comparison, string matching, string
  transforms, and aggregate operators — all following the same
  nesting rules as scalar fields.

  > NOTE: the `:all` operator is overloaded. Inside a comparison value
  > (e.g. `%{>: %{all: subquery}}`) it means subquery set comparison.
  > With `:in` on an array field (e.g. `%{tags: %{all: %{in: [...]}}}`)
  > it means "contains all values".

  ## Query operations

  Query operation keys are reserved. They map directly to Ecto query
  operations. Avoid using schema field names that match these keys.

  ### Select

      %{select: true}                          # SELECT p.*
      %{select: :id}                           # SELECT p.id
      %{select: [:id, :title]}                 # SELECT p.id, p.title
      %{select: %{map: [:id, :title]}}         # SELECT map(p, [:id, :title])
      %{select: %{map: %{custom_id: :id}}}     # SELECT p.id AS custom_id
      %{select: %{struct: [:id]}}              # SELECT struct(p, [:id])

      %{select_merge: %{map: %{custom_id: :id}}}

  Combine both:

      [select: %{map: [:id]}, select_merge: %{map: %{post_title: :title}}]

  ### Ordering

      %{order_by: :title}                           # ORDER BY title DESC
      %{order_by: [desc: :title]}                   # ORDER BY title DESC
      %{order_by: [asc: :title, desc: :id]}         # ORDER BY title ASC, id DESC
      %{prepend_order_by: :title}
      %{reverse_order: true}

  ### Grouping and having

      %{group_by: :author_id}
      %{group_by: [:author_id, :published]}

  The `:having` and `:or_having` keys accept the same filter language
  used for `:where`:

      %{having: %{views: %{avg: %{>: 10}}}}
      %{having: dynamic([p], p.views > ^10)}
      %{or_having: %{views: %{<: 5}}}

  ### Pagination

      %{limit: 10}
      %{offset: 5}
      %{first: 10}            # delegates to :limit

  The `:last` key wraps the entire query in a subquery that reverses
  the order and takes the last N rows:

      %{last: 2}                 # last 2 rows by primary key
      %{last: %{title: 2}}      # last 2 rows ordered by :title

  ### Distinct

      %{distinct: true}
      %{distinct: :title}
      %{distinct: [desc: :title]}

  ### Joins

  The `:join` key supports multiple join sources:

      %{join: [author: [as: :author]]}                                          # association
      %{join: [schema: [source: User, as: :user_join, on: true]]}               # schema
      %{join: [table: [source: "users", as: :users_table, on: true]]}           # table
      %{join: [query: [source: user_query, as: :adult_users, on: true]]}        # query
      %{join: [subquery: [source: user_query, as: :name, on: true]]}            # subquery
      %{join: [fragment: [source: %{name: :active_users, values: [min_age: 21]}, as: :active_users, on: true]]}

  ### Association shorthand

  When a key matches an association name, you can filter on the
  associated schema using a keyword list. This creates a join and
  applies the filters to the joined binding automatically:

      %{author: [as: :author, first_name: "John"]}
      %{author: [first_name: "John"]}
      %{author: [as: :author, type: :left, first_name: "John"]}

  The optional keys `:as`, `:on`, and `:type` configure the join.
  All remaining keys are treated as filters on the associated schema.

  ### Preload

      %{preload: :author}
      %{preload: [:author]}
      %{preload: [author: [:posts]]}
      %{preload: [bind: [as: [example: :author]]]}

  ### Set operations

      %{union: %{published: false}}
      %{union_all: %{published: false}}
      %{except: %{published: false}}
      %{intersect: %{published: false}}

  You can also pass a pre-built query:

      %{except: from(p in Post, where: p.published == ^false)}

  ### Subquery

  The `:subquery` key wraps the entire query (with all filters
  applied) as a subquery:

      %{subquery: %{id: 2}}

  ### Lock

      %{lock: fn query -> from(p in query, lock: "FOR UPDATE") end}
      %{lock: %{name: :for_share, values: []}}

  ### Common table expressions

      %{recursive_ctes: true}
      %{with_cte: [published_posts: [as: cte_query]]}
      %{with_cte: [published_posts: [as: %{source: Post, query: %{published: true}}]]}

  ### Named bindings

      %{with_named_binding: [author: %{join: [association: [source: :author, as: :author]]}]}

  ### Windows

      %{windows: [post_window: [partition_by: :author_id, order_by: [desc: :inserted_at]]]}

  ### With ties

      %{with_ties: true}
      %{with_ties: %{bind: %{as: %{post: true}}}}
      %{with_ties: %{bind: %{at: %{1 => true}}}}

  ### Update

      %{update: [set: [title: "After"], inc: [views: 1]]}
      %{update: %{set: %{title: "After"}}}

  ### Exclude

      %{exclude: :order_by}
      %{exclude: [:order_by, :limit]}

  ### Query prefix

      %{put_query_prefix: "tenant_a"}

  ## Binding selectors

  The `:bind` key targets a specific binding in a query. This is
  useful for queries with joins where you need to filter or apply
  operations on a joined table.

  Named bindings use `:as`:

      # Given a query: from(p in Post, as: :post)
      %{bind: %{as: %{post: %{published: true}}}}

  Positional bindings use `:at`:

      %{bind: %{at: %{1 => %{published: true}}}}

  Shortcut bindings use `:first` or `:last`:

      %{bind: %{first: %{published: true}}}      # targets the root from binding
      %{bind: %{last: %{first_name: "John"}}}     # targets the last join, or from if no joins

  You can target multiple bindings in a single call:

      %{bind: %{as: [post: %{published: true}, author: %{first_name: "John"}]}}

  Binding selectors work with query operations too:

      %{bind: %{as: %{author: %{order_by: %{asc: :first_name}}}}}
      %{bind: %{at: %{2 => %{group_by: :first_name}}}}
      %{bind: %{last: %{order_by: %{asc: :first_name}}}}

  ## Convenience filters

  These built-in keys provide common shortcuts:

      %{ids: [1, 2, 3]}                          # WHERE id IN (1, 2, 3)
      %{after: 10}                               # WHERE id > 10
      %{before: 10}                              # WHERE id < 10
      %{start_date: ~U[2026-01-01 00:00:00Z]}    # WHERE inserted_at >= value
      %{end_date: ~U[2026-12-31 23:59:59Z]}      # WHERE inserted_at <= value

  ## Dynamic expressions

  The `:dynamic` key accepts an `Ecto.Query.DynamicExpr` and applies
  it directly:

      %{dynamic: dynamic([p], p.views > ^10)}
      %{where: %{dynamic: dynamic([p], p.published == ^true)}}
      %{or_where: %{dynamic: dynamic([p], p.views > ^100)}}

  ## Exists

  The `:exists` key checks for the existence of rows in a subquery:

      %{where: %{exists: subquery_expr}}
      %{where: %{exists: %{not: subquery_expr}}}

  ## Source and query params

  The `:source` and `:query` meta-keys allow overriding the schema
  source and merging additional query params:

      [source: Post, id: 1]
      [source: Post, query: %{id: 1}]
      [source: "posts", query: %{select: [:id]}]

  When the source is a bare table name string with no schema module
  and no `:select` is provided, the system adds `select: true`
  automatically.

  ## Expression nesting

  Every field expression is a chain of nested maps. Each key type
  occupies one slot in the chain, processed from outermost to
  innermost:

      field -> negation -> aggregate -> operator -> value expression

  Here is an example that fills every slot:

      # field   negation   aggregate  operator  value expression
      %{views: %{not: %{avg:     %{>:      %{+: [:views, 10]}}}}}

  Not every slot needs to be filled. A simple equality filter only
  uses the field and a literal value (`%{title: "hello"}`). A
  comparison adds the operator slot (`%{views: %{>: 10}}`). Slots
  can be skipped — the system fills in defaults where needed.

  ## Processing order

  When a map contains multiple keys, the system sorts them into a
  fixed order before building the query:

      where / fields -> or_where -> query operations -> terminal filters

  * **where / fields** — explicit `:where` keys and bare field keys
    are processed first
  * **or_where** — explicit `:or_where` keys are processed second
  * **query operations** — reserved keys like `:limit`, `:order_by`,
    `:join`, `:preload` are processed next
  * **terminal filters** — `:last` and `:subquery` are always
    processed last because they wrap the entire query

  Input order does not matter. `%{last: 2, published: true, limit: 10}`
  produces the same query regardless of key order.

  ## Error handling

  This module follows a warn-and-skip model. Invalid filter data
  never raises an exception:

  * Unknown keys that are not schema fields or reserved keys are
    logged as warnings and skipped
  * Invalid payloads (wrong types, malformed maps) are logged as
    warnings and the failing operation is skipped
  * The rest of the query continues building normally — only the
    invalid entry is dropped

  This makes the module safe to use with user-provided data where
  some keys may be unexpected.

  See also `EctoShorts.Actions`, `EctoShorts.Dynamics`, and
  `EctoShorts.CommonFilters.Having`.
  """

  alias EctoShorts.CommonSchema
  alias EctoShorts.CommonQuery

  alias EctoShorts.CommonFilters.{
    BindingParams,
    Distinct,
    Filter,
    GroupBy,
    Having,
    Join,
    OrderBy,
    Preload,
    Select,
    SubQuery,
    WithCte,
    WithNamedBinding,
    WithTies,
    Windows,
    Update
  }

  alias EctoShorts.Logger
  alias EctoShorts.SchemaHelpers

  @logger_prefix "EctoShorts.CommonFilters"

  @binding_selector_key :bind
  @default_binding_selector {:as, nil}

  @where :where
  @map_payload_helper_operators [:datetime_add, :date_add, :from_now, :ago]
  @schema_filters [:where, :or_where]
  @query_filters [
    :distinct,
    :except,
    :except_all,
    :exclude,
    :first,
    :group_by,
    :having,
    :or_having,
    :intersect,
    :intersect_all,
    :union,
    :union_all,
    :join,
    :last,
    :lock,
    :limit,
    :offset,
    :put_query_prefix,
    :order_by,
    :windows,
    :reverse_order,
    :prepend_order_by,
    :preload,
    :recursive_ctes,
    :select,
    :select_merge,
    :subquery,
    :with_cte,
    :with_named_binding,
    :with_ties,
    :update
  ]

  @doc """
  Converts filter params into an `Ecto.Query`.

  `source` is a schema module, `{source, schema}` tuple, or an existing
  `Ecto.Query`. `params` is a map or keyword list of filter params.
  `opts` are forwarded to all sub-query builders.

  Schema field keys become `WHERE` conditions. Reserved query operation
  keys (`:limit`, `:order_by`, `:join`, etc.) become the corresponding
  Ecto query operations. Unknown keys log a warning and are skipped.
  Invalid payloads also log a warning and are skipped — the rest of the
  query continues building.

  Returns an `Ecto.Query` struct with all params applied.

  See the [moduledoc](`m:EctoShorts.CommonFilters`) for the complete
  filtering language reference, including
  [expression nesting](#module-expression-nesting),
  [processing order](#module-processing-order),
  and all supported [query operations](#module-query-operations).

  ## Options

  * `:repo` (default: `EctoShorts.Config.repo/0`) - the `Ecto.Repo`
    module used to resolve the dynamic expression adapter.
  * `:dynamic_adapter` - a module implementing
    `EctoShorts.Dynamics.Adapter` for this call. Defaults to the
    adapter resolved from `:repo`.
  * `:query_fields` - list of field atoms to limit which fields are
    accepted as schema filters.

  ## Examples

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{title: "Hello"}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post, where: p0.title == ^"Hello">

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{published: true, or_where: %{published: false}}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        where: p0.published == ^true or p0.published == ^false>

      iex> EctoShorts.CommonFilters.convert_params_to_filter(
      ...>   EctoShorts.Schema.Post,
      ...>   %{limit: 5, order_by: {:desc, :inserted_at}}
      ...> )
      #Ecto.Query<from p0 in EctoShorts.Schema.Post,
        order_by: [desc: p0.inserted_at], limit: ^5>

  See also `EctoShorts.Actions.all/3`, `EctoShorts.Dynamics`, and
  `EctoShorts.CommonFilters.Having`.
  """
  @spec convert_params_to_filter(
          source :: module() | {binary(), module()} | Ecto.Query.t(),
          params :: map() | keyword(),
          opts :: keyword()
        ) :: Ecto.Query.t()
  def convert_params_to_filter(source, params, opts \\ [])

  def convert_params_to_filter(source, params, opts) when is_map(params) do
    convert_params_to_filter(source, Map.to_list(params), opts)
  end

  def convert_params_to_filter(source, entries, opts) when is_list(entries) do
    query = CommonSchema.to_query(source)

    has_source_or_query? =
      Keyword.has_key?(entries, :source) or Keyword.has_key?(entries, :query)

    if Keyword.keyword?(entries) do
      {schema_source, params} = Keyword.pop(entries, :source, source)

      {other_params, params} = Keyword.pop(params, :query, [])

      normalized_source = CommonSchema.normalize_source(schema_source)

      merged_params =
        other_params
        |> ensure_kw()
        |> Keyword.merge(params)

      merged_params =
        case {has_source_or_query?, normalized_source} do
          {true, {_table, nil}} ->
            if Keyword.has_key?(merged_params, :select) do
              merged_params
            else
              Keyword.put(merged_params, :select, true)
            end

          _ ->
            merged_params
        end

      do_convert(normalized_source, query, merged_params, opts)
    else
      Enum.reduce(entries, query, fn entry, query_acc ->
        do_convert(source, query_acc, entry, opts)
      end)
    end
  end

  defp ensure_kw(map) when is_map(map), do: Map.to_list(map)

  defp ensure_kw(list) when is_list(list) do
    if Keyword.keyword?(list) do
      list
    else
      Logger.warning(@logger_prefix, "Expected params to be a keyword list, got: #{inspect(list)}")
      []
    end
  end

  defp ensure_kw(term) do
    Logger.warning(@logger_prefix, "Expected params to be a keyword list, got: #{inspect(term)}")
    []
  end

  defp do_convert(schema_source, query, params, opts) do
    normalized_params = normalize_filter_params(params)

    if is_map(normalized_params) or Keyword.keyword?(normalized_params) do
      create_schema_filter(
        schema_source,
        query,
        @default_binding_selector,
        @where,
        normalized_params,
        opts
      )
    else
      Logger.warning(@logger_prefix, "Expected params to be a map or list, got: #{inspect(params)}")
      query
    end
  end

  @doc false
  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        _filter_op,
        {key, value},
        opts
      )
      when key in @schema_filters do
    reduce_schema_filter_params(schema_source, query, binding_selector, key, value, opts)
  end

  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {@binding_selector_key, bind_params},
        opts
      ) do
    BindingParams.build_binding_params(
      schema_source,
      query,
      binding_selector,
      filter_op,
      bind_params,
      opts
    )
  end

  def create_schema_filter(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {key, value},
        opts
      ) do
    query_filters = Keyword.get(opts, :query_filters, @query_filters)

    if key in query_filters do
      build_query(schema_source, query, binding_selector, key, value, opts)
    else
      reduce_default_filter_params(
        schema_source,
        query,
        binding_selector,
        filter_op,
        key,
        value,
        opts
      )
    end
  end

  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts)
      when is_map(params) do
    create_schema_filter(
      schema_source,
      query,
      binding_selector,
      filter_op,
      Map.to_list(params),
      opts
    )
  end

  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts)
      when is_list(params) do
    if Keyword.keyword?(params) do
      Enum.reduce(params, query, fn {key, value}, query_acc ->
        create_schema_filter(
          schema_source,
          query_acc,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )
      end)
    else
      build_query(schema_source, query, binding_selector, filter_op, params, opts)
    end
  end

  @doc false
  def create_schema_filter(schema_source, query, binding_selector, filter_op, params, opts) do
    build_query(schema_source, query, binding_selector, filter_op, params, opts)
  end

  defp reduce_schema_filter_params(schema_source, query, binding_selector, filter_op, value, opts) do
    if is_map(value) or is_list(value) do
      Enum.reduce(value, query, fn entry, query_acc ->
        build_schema_filters(schema_source, query_acc, binding_selector, filter_op, entry, opts)
      end)
    else
      Logger.warning(
        @logger_prefix,
        "Expected params for #{filter_op} to be a map or keyword list, got: #{inspect(value)}"
      )

      query
    end
  end

  defp reduce_default_filter_params(
         schema_source,
         query,
         binding_selector,
         filter_op,
         key,
         value,
         opts
       ) do
    case CommonSchema.get_schema_reflection(schema_source, :associations) do
      nil ->
        build_schema_filters(
          schema_source,
          query,
          binding_selector,
          filter_op,
          {key, value},
          opts
        )

      assocs ->
        if key in assocs do
          build_join_filters(
            schema_source,
            query,
            binding_selector,
            filter_op,
            key,
            value,
            opts
          )
        else
          build_schema_filters(
            schema_source,
            query,
            binding_selector,
            filter_op,
            {key, value},
            opts
          )
        end
    end
  end

  defp build_join_filters(
         schema_source,
         query,
         binding_selector,
         filter,
         assoc_key,
         params,
         opts
       ) do
    if Keyword.keyword?(params) do
      assoc_schema =
        case schema_source do
          {_, parent_schema} -> SchemaHelpers.get_related_schema(parent_schema, assoc_key)
          parent_schema -> SchemaHelpers.get_related_schema(parent_schema, assoc_key)
        end

      joined_query =
        build_query(
          schema_source,
          query,
          binding_selector,
          :join,
          [{assoc_key, params}],
          opts
        )

      {join_binding_mode, join_binding_target} =
        if Keyword.has_key?(params, :as) do
          {:as, Keyword.get(params, :as, nil)}
        else
          {:at, CommonQuery.query_binding_count(joined_query)}
        end

      create_schema_filter(
        assoc_schema,
        joined_query,
        {join_binding_mode, join_binding_target},
        filter,
        Keyword.drop(params, [:as, :on, :type]),
        opts
      )
    else
      Logger.warning(
        @logger_prefix,
        "Expected association params for #{inspect(assoc_key)} to be a keyword list, got: #{inspect(params)}"
      )

      query
    end
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_map(value) and not is_struct(value) and
              (is_map_key(value, :datetime) or is_map_key(value, :date)) do
    build_query(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, value},
      opts
    )
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_map(value) and not is_struct(value) do
    create_schema_filter(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, Map.to_list(value)},
      opts
    )
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       )
       when is_list(value) do
    if Keyword.keyword?(value) do
      Enum.reduce(value, query, fn {key2, value2}, query_acc ->
        create_schema_filter(
          schema_source,
          query_acc,
          binding_selector,
          filter_op,
          {key, {key2, value2}},
          opts
        )
      end)
    else
      build_query(
        schema_source,
        query,
        binding_selector,
        filter_op,
        {key, value},
        opts
      )
    end
  end

  defp build_schema_filters(
         schema_source,
         query,
         binding_selector,
         filter_op,
         {key, value},
         opts
       ) do
    build_query(
      schema_source,
      query,
      binding_selector,
      filter_op,
      {key, value},
      opts
    )
  end

  @query_builder_modules %{
    distinct: Distinct,
    group_by: GroupBy,
    having: Having,
    or_having: Having,
    join: Join,
    order_by: OrderBy,
    prepend_order_by: OrderBy,
    preload: Preload,
    select: Select,
    select_merge: Select,
    update: Update,
    windows: Windows,
    with_cte: WithCte,
    with_named_binding: WithNamedBinding,
    with_ties: WithTies
  }

  defp build_query(schema_source, query, binding_selector, :subquery, params, opts)
       when is_map(params) or is_list(params) do
    binding_source = to_binding_source(schema_source, query, binding_selector)

    filtered_query =
      create_schema_filter(
        schema_source,
        query,
        binding_selector,
        @where,
        params,
        opts
      )

    SubQuery.build(
      binding_source,
      :subquery,
      filtered_query,
      binding_selector,
      params,
      opts
    )
  end

  defp build_query(_schema_source, query, _binding_selector, :subquery, params, _opts) do
    Logger.warning(
      @logger_prefix,
      "Expected :subquery params to be a map or keyword list, got: #{inspect(params)}"
    )

    query
  end

  defp build_query(schema_source, query, binding_selector, filter_op, params, opts) do
    binding_source = to_binding_source(schema_source, query, binding_selector)
    module = Map.get(@query_builder_modules, filter_op, Filter)
    module.build(binding_source, filter_op, query, binding_selector, params, opts)
  end

  defp to_binding_source(schema_source, _query, {:as, nil}) do
    schema_source
  end

  defp to_binding_source(_schema_source, query, {_binding_mode, binding_target}) do
    CommonQuery.get_query_binding_source(query, binding_target)
  end

  defp normalize_filter_params({k, v})
       when k in @map_payload_helper_operators and is_map(v) and not is_struct(v) do
    {k, v}
  end

  defp normalize_filter_params({k, v}) when is_map(v) or is_list(v) do
    {k, normalize_filter_params(v)}
  end

  defp normalize_filter_params(map) when is_map(map) and not is_struct(map) do
    map
    |> Map.to_list()
    |> normalize_filter_params()
  end

  defp normalize_filter_params(list) when is_list(list) do
    if Keyword.keyword?(list) do
      list
      |> sort_params()
      |> Enum.map(fn {k, v} -> {k, normalize_filter_params(v)} end)
    else
      list
    end
  end

  defp normalize_filter_params(term) do
    term
  end

  defp sort_params(params) do
    where_filters = Keyword.take(params, [:where])
    or_where_filters = Keyword.take(params, [:or_where])
    terminal_filters = Enum.filter(params, fn {key, _val} -> key in [:last, :subquery] end)

    # Regular field filters should be processed with where_filters
    # since they can contain implicit WHERE clauses and must come
    # before or_where filters
    rest = Keyword.drop(params, [:where, :or_where, :last, :subquery])

    where_filters
    |> Kernel.++(rest)
    |> Kernel.++(or_where_filters)
    |> Kernel.++(terminal_filters)
  end
end
