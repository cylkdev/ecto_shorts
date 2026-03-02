defmodule EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.DateTime do
  @moduledoc since: "3.0.0"
  @moduledoc false

  @behaviour EctoShorts.Compiler.ClauseSpecProvider

  alias EctoShorts.Compiler.AST
  alias EctoShorts.Compiler.ClauseSpec

  @comparison_operators [:==, :!=, :>, :>=, :<, :<=]
  @date_time_helpers [
    {:datetime, :add},
    {:datetime, :ago},
    {:datetime, :from_now},
    {:date, :add}
  ]

  @doc false
  @impl true
  def clause_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    date_time_specs(context, binding_head_ast, target_binding_var, binding_body_asts)
  end

  @doc false
  def date_time_specs(context, binding_head_ast, target_binding_var, binding_body_asts) do
    key_var = Macro.var(:key, context)
    op_var = Macro.var(:op, context)
    payload_var = Macro.var(:payload, context)
    rhs_dynamic_var = Macro.var(:rhs_dynamic, context)
    field_ast = AST.field_ast(target_binding_var, key_var)

    op_guard =
      quote do
        unquote(op_var) in unquote(@comparison_operators)
      end

    Enum.flat_map(@date_time_helpers, fn {wrapper, operation} ->
      helper_expr_ast =
        quote(do: {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]})

      rhs_dynamic_expr_ast =
        date_time_dynamic_expr_ast(binding_body_asts, target_binding_var, helper_expr_ast)

      [
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head:
            quote(do: {unquote(op_var), {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}),
          guard: op_guard,
          body:
            quote do
              unquote(rhs_dynamic_var) = unquote(rhs_dynamic_expr_ast)

              unquote(
                date_time_comparison_case_ast(
                  binding_body_asts,
                  field_ast,
                  op_var,
                  rhs_dynamic_var
                )
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head:
            quote(
              do: {:not, {unquote(op_var), {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}}
            ),
          guard: op_guard,
          body:
            quote do
              unquote(rhs_dynamic_var) = unquote(rhs_dynamic_expr_ast)

              unquote(
                not_date_time_comparison_case_ast(
                  binding_body_asts,
                  field_ast,
                  op_var,
                  rhs_dynamic_var
                )
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}),
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:==, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {:not, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}),
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:!=, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head:
            quote(do: {unquote(op_var), %{unquote(wrapper) => %{unquote(operation) => unquote(payload_var)}}}),
          guard: op_guard,
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {unquote(op_var), {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head:
            quote(
              do:
                {:not,
                 {unquote(op_var), %{unquote(wrapper) => %{unquote(operation) => unquote(payload_var)}}}}
            ),
          guard: op_guard,
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:not, {unquote(op_var), {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: %{unquote(wrapper) => %{unquote(operation) => unquote(payload_var)}}),
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:==, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        },
        %ClauseSpec{
          binding_head: binding_head_ast,
          key: key_var,
          head: quote(do: {:not, %{unquote(wrapper) => %{unquote(operation) => unquote(payload_var)}}}),
          body:
            quote do
              apply_dynamic_expr(
                unquote(binding_head_ast),
                unquote(key_var),
                {:!=, {unquote(wrapper), [{unquote(operation), unquote(payload_var)}]}}
              )
            end
        }
      ]
    end)
  end

  defp date_time_comparison_case_ast(binding_body_asts, field_ast, op_var, rhs_dynamic_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) == ^unquote(rhs_dynamic_var)))
          )

        :!= ->
          unquote(
            AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) != ^unquote(rhs_dynamic_var)))
          )

        :> ->
          unquote(
            AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) > ^unquote(rhs_dynamic_var)))
          )

        :>= ->
          unquote(
            AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) >= ^unquote(rhs_dynamic_var)))
          )

        :< ->
          unquote(
            AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) < ^unquote(rhs_dynamic_var)))
          )

        :<= ->
          unquote(
            AST.dynamic_ast(binding_body_asts, quote(do: unquote(field_ast) <= ^unquote(rhs_dynamic_var)))
          )
      end
    end
  end

  defp not_date_time_comparison_case_ast(binding_body_asts, field_ast, op_var, rhs_dynamic_var) do
    quote do
      case unquote(op_var) do
        :== ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) == ^unquote(rhs_dynamic_var)))
            )
          )

        :!= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) != ^unquote(rhs_dynamic_var)))
            )
          )

        :> ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) > ^unquote(rhs_dynamic_var)))
            )
          )

        :>= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) >= ^unquote(rhs_dynamic_var)))
            )
          )

        :< ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) < ^unquote(rhs_dynamic_var)))
            )
          )

        :<= ->
          unquote(
            AST.dynamic_ast(
              binding_body_asts,
              quote(do: not (unquote(field_ast) <= ^unquote(rhs_dynamic_var)))
            )
          )
      end
    end
  end

  defp date_time_dynamic_expr_ast(binding_body_asts, target_binding_var, helper_expr_ast) do
    quote do
      normalize_payload = fn
        label, payload when is_map(payload) and not is_struct(payload) ->
          payload

        label, payload when is_list(payload) ->
          if Keyword.keyword?(payload) do
            Map.new(payload)
          else
            raise ArgumentError,
                  "Expected #{inspect(label)} payload to be a map or keyword list, got: #{inspect(payload)}"
          end

        label, payload ->
          raise ArgumentError,
                "Expected #{inspect(label)} payload to be a map or keyword list, got: #{inspect(payload)}"
      end

      build_date_time_expr = fn build_date_time_expr, expr ->
        case expr do
          {:datetime, [{:from_now, payload}]} ->
            payload = normalize_payload.({:datetime, :from_now}, payload)
            count = Map.fetch!(payload, :count)
            interval = Map.fetch!(payload, :interval)
            unquote(AST.dynamic_ast(binding_body_asts, quote(do: from_now(^count, ^interval))))

          {:datetime, [{:ago, payload}]} ->
            payload = normalize_payload.({:datetime, :ago}, payload)
            count = Map.fetch!(payload, :count)
            interval = Map.fetch!(payload, :interval)
            unquote(AST.dynamic_ast(binding_body_asts, quote(do: ago(^count, ^interval))))

          {:datetime, [{:add, payload}]} ->
            payload = normalize_payload.({:datetime, :add}, payload)
            field_expr = Map.fetch!(payload, :field)
            count = Map.fetch!(payload, :count)
            interval = Map.fetch!(payload, :interval)
            field_dynamic = build_date_time_expr.(build_date_time_expr, field_expr)

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote(do: datetime_add(^field_dynamic, ^count, ^interval))
              )
            )

          {:date, [{:add, payload}]} ->
            payload = normalize_payload.({:date, :add}, payload)
            field_expr = Map.fetch!(payload, :field)
            count = Map.fetch!(payload, :count)
            interval = Map.fetch!(payload, :interval)
            field_dynamic = build_date_time_expr.(build_date_time_expr, field_expr)

            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote(do: date_add(^field_dynamic, ^count, ^interval))
              )
            )

          %{datetime: inner} when is_map(inner) and map_size(inner) === 1 ->
            inner_list = Map.to_list(inner)
            build_date_time_expr.(build_date_time_expr, {:datetime, inner_list})

          %{date: inner} when is_map(inner) and map_size(inner) === 1 ->
            inner_list = Map.to_list(inner)
            build_date_time_expr.(build_date_time_expr, {:date, inner_list})

          field_name when is_atom(field_name) ->
            unquote(
              AST.dynamic_ast(
                binding_body_asts,
                quote do
                  field(unquote(target_binding_var), ^field_name)
                end
              )
            )

          literal ->
            unquote(AST.dynamic_ast(binding_body_asts, quote(do: ^literal)))
        end
      end

      build_date_time_expr.(build_date_time_expr, unquote(helper_expr_ast))
    end
  end
end
