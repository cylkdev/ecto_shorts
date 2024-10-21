defmodule EctoShorts.QueryBuilder.CommonTest do
  use ExUnit.Case, async: true
  doctest EctoShorts.QueryBuilder.Common

  alias Ecto.Query
  alias EctoShorts.{
    QueryBuilder.Common,
    Support.Schemas.Post
  }

  require Ecto.Query

  describe "create_schema_filters: " do
    test "arg queryable - returns query built by search/2 callback" do
      assert %Ecto.Query{
        from: %Ecto.Query.FromExpr{
          prefix: nil,
          source: {"posts", EctoShorts.Support.Schemas.Post}
        },
        wheres: [
          %Ecto.Query.BooleanExpr{
            expr: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
            op: :and,
            params: [{1, {0, :id}}],
            subqueries: []
          }
        ]
      } = Query.from(p in Post, where: p.id == ^1)

      assert %Ecto.Query{
        from: %Ecto.Query.FromExpr{
          prefix: nil,
          source: {"posts", EctoShorts.Support.Schemas.Post}
        },
        wheres: [
          %Ecto.Query.BooleanExpr{
            expr: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
            op: :and,
            params: [{1, {0, :id}}],
            subqueries: []
          }
        ]
      } = Common.create_schema_filter(Post, :search, %{id: 1})
    end

    test "arg {source, queryable} - returns query built by search/2 callback" do
      assert %Ecto.Query{
        from: %Ecto.Query.FromExpr{
          prefix: nil,
          source: {"posts", EctoShorts.Support.Schemas.Post}
        },
        wheres: [
          %Ecto.Query.BooleanExpr{
            expr: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
            op: :and,
            params: [{1, {0, :id}}],
            subqueries: []
          }
        ]
      } = Query.from(p in Post, where: p.id == ^1)

      assert %Ecto.Query{
        from: %Ecto.Query.FromExpr{
          prefix: nil,
          source: {"posts", EctoShorts.Support.Schemas.Post}
        },
        wheres: [
          %Ecto.Query.BooleanExpr{
            expr: {:==, [], [{{:., [], [{:&, [], [0]}, :id]}, [], []}, {:^, [], [0]}]},
            op: :and,
            params: [{1, {0, :id}}],
            subqueries: []
          }
        ]
      } = Common.create_schema_filter({"posts", Post}, :search, %{id: 1})
    end
  end
end
