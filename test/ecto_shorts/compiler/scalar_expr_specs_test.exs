defmodule EctoShorts.Dynamics.Adapters.Postgres.ScalarExprSpecsTest do
  use ExUnit.Case, async: true

  alias EctoShorts.Compiler.ClauseBuilder
  alias EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Core, as: ScalarExprSpecs
  alias EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Aggregate, as: AggregateSpecs
  alias EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Arithmetic, as: ArithmeticSpecs
  alias EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.DateTime, as: DateTimeSpecs
  alias EctoShorts.Dynamics.Adapters.Postgres.ScalarExpr.Specs.Quantifier, as: QuantifierSpecs

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defp compile_specs_module!(specs) do
    clause_asts = Enum.map(specs, &ClauseBuilder.clause_ast/1)

    module =
      Module.concat([__MODULE__, :"Tmp#{System.unique_integer([:positive])}"])

    quoted =
      quote do
        defmodule unquote(module) do
          import Ecto.Query
          require Ecto.Query

          unquote_splicing(clause_asts)
        end
      end

    Code.compile_quoted(quoted)
    module
  end

  defp binding_setup(context) do
    binding_head_ast = quote(do: {:as, nil})
    target_binding_var = Macro.var(:q, context)
    binding_body_asts = [quote(do: unquote(target_binding_var))]

    {binding_head_ast, target_binding_var, binding_body_asts}
  end

  test "nil_specs/4 builds clauses without needing other groups" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ScalarExprSpecs.nil_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )

    module = compile_specs_module!(specs)

    key = :archived_at

    expected_is_nil = dynamic([q], is_nil(field(q, ^key)))
    expected_not_nil = dynamic([q], not is_nil(field(q, ^key)))

    assert_dynamic(
      expected_is_nil,
      module.apply_dynamic_expr({:as, nil}, key, {:eq, nil})
    )

    assert_dynamic(
      expected_not_nil,
      module.apply_dynamic_expr({:as, nil}, key, {:!=, nil})
    )
  end

  test "list_semantic_specs/2 coerces == with list to :in" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ScalarExprSpecs.list_semantic_specs(__MODULE__, binding_head_ast, target_binding_var, binding_body_asts) ++
        ScalarExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :status

    expected_in = dynamic([q], field(q, ^key) in ^[1, 2])
    expected_not_in = dynamic([q], field(q, ^key) not in ^[1, 2])

    assert_dynamic(
      expected_in,
      module.apply_dynamic_expr({:as, nil}, key, {:==, [1, 2]})
    )

    assert_dynamic(
      expected_not_in,
      module.apply_dynamic_expr({:as, nil}, key, {:!=, [1, 2]})
    )
  end

  test "like_ilike_specs/4 builds like/ilike clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ScalarExprSpecs.like_ilike_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )

    module = compile_specs_module!(specs)

    key = :title

    expected_like = dynamic([q], like(field(q, ^key), ^"%foo%"))

    assert_dynamic(
      expected_like,
      module.apply_dynamic_expr({:as, nil}, key, {:like, "foo"})
    )

    patterns = ["%foo%", "%bar%"]
    expected_like_list = dynamic([q], fragment("? LIKE ANY(?)", field(q, ^key), ^patterns))

    assert_dynamic(
      expected_like_list,
      module.apply_dynamic_expr({:as, nil}, key, {:like, ["foo", "bar"]})
    )
  end

  test "lower_upper_specs/4 builds lower/upper comparison clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ScalarExprSpecs.lower_upper_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) ++
        ScalarExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :title

    expected_lower = dynamic([q], fragment("lower(?)", field(q, ^key)) == ^"foo")
    expected_upper = dynamic([q], fragment("upper(?)", field(q, ^key)) == ^"FOO")

    assert_dynamic(
      expected_lower,
      module.apply_dynamic_expr({:as, nil}, key, {:lower, "foo"})
    )

    assert_dynamic(
      expected_upper,
      module.apply_dynamic_expr({:as, nil}, key, {:upper, "FOO"})
    )
  end

  test "date_time_specs/4 builds datetime comparison clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      DateTimeSpecs.date_time_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) ++
        ScalarExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :inserted_at
    payload = %{field: :inserted_at, count: 1, interval: "day"}

    expected =
      dynamic(
        [q],
        field(q, ^key) >= datetime_add(field(q, ^key), ^1, ^"day")
      )

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:>=, {:datetime, [{:add, payload}]}})
    )
  end

  test "arithmetic_specs/4 builds arithmetic comparison clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      ArithmeticSpecs.arithmetic_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      )

    module = compile_specs_module!(specs)

    key = :views

    expected = dynamic([q], field(q, ^key) > field(q, ^key) + ^10)

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:>, {:+, [:views, 10]}})
    )
  end

  test "any_specs/4 builds any-subquery comparison clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      QuantifierSpecs.any_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) ++
        ScalarExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :id
    subquery_expr = from(c in "comments", select: c.post_id)

    expected = dynamic([q], field(q, ^key) > any(subquery_expr))

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:any, {:>, subquery_expr}})
    )
  end

  test "all_specs/4 builds all-subquery comparison clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      QuantifierSpecs.all_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) ++
        ScalarExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :id
    subquery_expr = from(c in "comments", select: c.post_id)

    expected = dynamic([q], field(q, ^key) > all(subquery_expr))

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:all, {:>, subquery_expr}})
    )
  end

  test "aggregate_specs/4 builds aggregate comparison clauses" do
    {binding_head_ast, target_binding_var, binding_body_asts} = binding_setup(__MODULE__)

    specs =
      AggregateSpecs.aggregate_specs(
        __MODULE__,
        binding_head_ast,
        target_binding_var,
        binding_body_asts
      ) ++
        ScalarExprSpecs.base_op_specs(
          __MODULE__,
          binding_head_ast,
          target_binding_var,
          binding_body_asts
        )

    module = compile_specs_module!(specs)

    key = :views

    expected = dynamic([q], avg(field(q, ^key)) > ^10)

    assert_dynamic(
      expected,
      module.apply_dynamic_expr({:as, nil}, key, {:avg, {:>, 10}})
    )
  end
end
