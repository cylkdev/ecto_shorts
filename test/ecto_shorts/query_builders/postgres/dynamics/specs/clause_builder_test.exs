defmodule EctoShorts.QueryBuilder.Dynamics.Expression.ClauseBuilderTest do
  use ExUnit.Case, async: true

  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseBuilder
  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseEmitter
  alias EctoShorts.QueryBuilder.Dynamics.Expression.ClauseSpec
  alias EctoShorts.QueryBuilder.Dynamics.Expression.Emitters.DynamicFieldExpr

  import Ecto.Query
  import EctoShorts.Testing, only: [assert_dynamic: 2]

  defmodule KindCapturingEmitter do
    @moduledoc false

    @behaviour ClauseEmitter

    @impl true
    def quoted_def(kind, _binding_head, _key, _head, _body, _guard) do
      quote do
        def emitted_kind, do: unquote(kind)
      end
    end
  end

  defp compile_clause_module!(clause_ast) do
    # Generate a unique module name to avoid conflicts across async tests.
    module = Module.concat([__MODULE__, :"Tmp#{System.unique_integer([:positive])}"])

    quoted =
      quote do
        # The generated clause uses `field/2` and `fragment/1`, so import Ecto.Query.
        defmodule unquote(module) do
          import Ecto.Query
          require Ecto.Query

          unquote(clause_ast)
        end
      end

    # Compile the quoted module into the VM and return its module name so the
    # test can call the generated function via `apply/3`.
    Code.compile_quoted(quoted)
    module
  end

  test "clause_ast/2 builds a scalar equality clause and it compiles" do
    # `:kind` is a tag field on the spec.
    # ClauseBuilder does not branch on it.
    key_var = Macro.var(:key, nil)
    v_var = Macro.var(:v, nil)

    spec =
      ClauseSpec.new!(%{
        kind: :clause,
        binding_head: quote(do: {:as, nil}),
        key: key_var,
        head: quote(do: {:==, unquote(v_var)}),
        body:
          quote do
            Ecto.Query.dynamic([q], field(q, ^unquote(key_var)) == ^unquote(v_var))
          end
      })

    assert {:ok, clause_ast} = ClauseBuilder.clause_ast(DynamicFieldExpr, spec)

    module = compile_clause_module!(clause_ast)

    # Build the "literal" expected expression using the Ecto DSL.
    #
    # `assert_dynamic/2` compares dynamics via `Macro.to_string/1`, which gives a
    # stable AST-level assertion without needing a repo connection.
    expected_dyn = dynamic([q], field(q, ^:title) == ^"hello")

    # Call the generated function clause and compare to the expected dynamic.
    actual_dyn = apply(module, :dynamic_field_expr, [{:as, nil}, :title, {:==, "hello"}])
    assert_dynamic(expected_dyn, actual_dyn)
  end

  test "clause_ast/2 builds a clause with a guard and it compiles" do
    # ClauseSpec supports an optional `:guard` AST.
    key_var = Macro.var(:key, nil)
    values_var = Macro.var(:values, nil)

    spec =
      ClauseSpec.new!(%{
        kind: :clause,
        binding_head: quote(do: {:as, nil}),
        key: key_var,
        head: quote(do: {:==, unquote(values_var)}),
        guard: quote(do: is_list(unquote(values_var))),
        body:
          quote do
            Ecto.Query.dynamic([q], field(q, ^unquote(key_var)) in ^unquote(values_var))
          end
      })

    assert {:ok, clause_ast} = ClauseBuilder.clause_ast(DynamicFieldExpr, spec)

    # Sanity check the emitted source includes the guard.
    assert Macro.to_string(clause_ast) |> String.contains?("when is_list(values)")

    module = compile_clause_module!(clause_ast)
    expected_dyn = dynamic([q], field(q, ^:id) in ^[1, 2])
    actual_dyn = apply(module, :dynamic_field_expr, [{:as, nil}, :id, {:==, [1, 2]}])
    assert_dynamic(expected_dyn, actual_dyn)
  end

  test "clause_ast/2 returns error when kind is not an atom" do
    key_var = Macro.var(:key, nil)
    v_var = Macro.var(:v, nil)

    spec = %{
      kind: 123,
      binding_head: quote(do: {:as, nil}),
      key: key_var,
      head: quote(do: {:==, unquote(v_var)}),
      body:
        quote do
          Ecto.Query.dynamic([q], field(q, ^unquote(key_var)) == ^unquote(v_var))
        end
    }

    assert {:error,
            %NimbleOptions.ValidationError{
              message: "invalid value for :kind option: expected atom, got: 123",
              key: :kind,
              value: 123,
              keys_path: []
            }} = ClauseBuilder.clause_ast(DynamicFieldExpr, spec)
  end

  test "clause_ast/2 returns error for missing keys" do
    assert {:error,
            %NimbleOptions.ValidationError{
              message: "required :binding_head option not found, received options: [:kind]",
              key: :binding_head,
              value: nil,
              keys_path: []
            }} = ClauseBuilder.clause_ast(DynamicFieldExpr, %{kind: :clause})
  end

  test "clause_ast/2 returns error for invalid emitter" do
    spec =
      ClauseSpec.new!(%{
        kind: :clause,
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: :anything),
        body: quote(do: :ok)
      })

    assert {:error, :invalid_emitter} = ClauseBuilder.clause_ast(__MODULE__, spec)
  end

  test "clause_ast/2 delegates to the given emitter and passes kind" do
    spec =
      ClauseSpec.new!(%{
        kind: :my_kind,
        binding_head: quote(do: {:as, nil}),
        key: Macro.var(:key, nil),
        head: quote(do: :anything),
        body: quote(do: :ok)
      })

    assert {:ok, clause_ast} = ClauseBuilder.clause_ast(KindCapturingEmitter, spec)
    module = compile_clause_module!(clause_ast)
    assert apply(module, :emitted_kind, []) == :my_kind
  end
end
