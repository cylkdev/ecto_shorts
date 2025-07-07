defmodule EctoShorts.CommonQueryAPI.JoinBuilder do
  @moduledoc false

  @max_positional_bindings Application.compile_env(
                             EctoShorts.Config.app(),
                             :max_positional_bindings
                           ) || 10

  defmacro define_base_api(before \\ []) do
    blocks = build_base_ast(before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_positional_binding_api(before \\ []) do
    blocks = build_positional_binding_ast(before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  defmacro define_named_binding_api(before \\ []) do
    blocks = build_named_binding_ast(before, __CALLER__)

    quote do
      unquote(blocks)
    end
  end

  @doc """
  ...
  """
  def build_base_ast(before, env) do
    var_elem = Macro.var(:b, env.context)

    [
      quote do
        def join_subquery(query, :first, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, :first, opts[:on] || true, opts)

          inner_query = params.query

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [unquote(var_elem)],
            x in subquery(inner_query),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_query(query, :first, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, :first, opts[:on] || true, opts)

          value = params.value

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [unquote(var_elem)],
            x in ^value,
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_association(query, :last, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, :last, opts[:on] || true, opts)

          key = params.key

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [_, ..., unquote(var_elem)],
            x in assoc(unquote(var_elem), ^key),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_subquery(query, :last, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, :last, opts[:on] || true, opts)

          inner_query = params.query

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [_, ..., unquote(var_elem)],
            x in subquery(inner_query),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_query(query, :last, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, :last, opts[:on] || true, opts)

          value = params.value

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [_, ..., unquote(var_elem)],
            x in ^value,
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end
    ]
  end

  @doc """
  ...
  """
  def build_positional_binding_ast(before, env) do

    Enum.flat_map(1..@max_positional_bindings, fn count ->
      var_names = Enum.map(1..count, fn i -> :"b#{i - 1}" end)
      var_binding = Enum.map(var_names, fn name -> Macro.var(name, env.context) end)
      var_elem = List.last(var_binding)

      [
        quote do
          def join_association(query, unquote(count), qual, params, opts) do
            as = opts[:as]
            prefix = opts[:prefix]

            source = params.source
            on = __MODULE__.dynamic(source, unquote(count), opts[:on] || true, opts)

            key = params.key

            unquote_splicing(before)

            Ecto.Query.join(
              query,
              qual,
              [unquote_splicing(var_binding)],
              x in assoc(unquote(var_elem), ^key),
              as: ^as,
              on: ^on,
              prefix: ^prefix
            )
          end
        end,
        quote do
          def join_subquery(query, unquote(count), qual, params, opts) do
            as = opts[:as]
            prefix = opts[:prefix]

            source = params.source
            on = __MODULE__.dynamic(source, unquote(count), opts[:on] || true, opts)

            inner_query = params.query

            unquote_splicing(before)

            Ecto.Query.join(
              query,
              qual,
              [unquote_splicing(var_binding)],
              x in subquery(inner_query),
              as: ^as,
              on: ^on,
              prefix: ^prefix
            )
          end
        end,
        quote do
          def join_query(query, unquote(count), qual, params, opts) do
            as = opts[:as]
            prefix = opts[:prefix]

            source = params.source
            on = __MODULE__.dynamic(source, unquote(count), opts[:on] || true, opts)

            value = params.value

            unquote_splicing(before)

            Ecto.Query.join(
              query,
              qual,
              [unquote_splicing(var_binding)],
              x in ^value,
              as: ^as,
              on: ^on,
              prefix: ^prefix
            )
          end
        end
      ]
    end)
  end

  @doc """
  ...
  """
  def build_named_binding_ast(before, env) do
    var_elem = Macro.var(:b, env.context)

    [
      quote do
        def join_association(query, nil, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, nil, opts[:on] || true, opts)

          key = params.key

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [unquote(var_elem)],
            x in assoc(unquote(var_elem), ^key),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_association(query, binding_alias, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, binding_alias, opts[:on] || true, opts)

          key = params.key

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [{^binding_alias, unquote(var_elem)}],
            x in assoc(unquote(var_elem), ^key),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_subquery(query, nil, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, nil, opts[:on] || true, opts)

          inner_query = params.query

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [unquote(var_elem)],
            x in subquery(inner_query),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_subquery(query, binding_alias, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, binding_alias, opts[:on] || true, opts)

          inner_query = params.query

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [{^binding_alias, unquote(var_elem)}],
            x in subquery(inner_query),
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_query(query, nil, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, nil, opts[:on] || true, opts)

          value = params.value

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [unquote(var_elem)],
            x in ^value,
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end,
      quote do
        def join_query(query, binding_alias, qual, params, opts) do
          as = opts[:as]
          prefix = opts[:prefix]

          source = params.source
          on = __MODULE__.dynamic(source, binding_alias, opts[:on] || true, opts)

          value = params.value

          unquote_splicing(before)

          Ecto.Query.join(
            query,
            qual,
            [{^binding_alias, unquote(var_elem)}],
            x in ^value,
            as: ^as,
            on: ^on,
            prefix: ^prefix
          )
        end
      end
    ]
  end
end
