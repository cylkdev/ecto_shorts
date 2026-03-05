defmodule EctoShorts.DynamicsTest do
  use ExUnit.Case, async: true
  use EctoShorts.Testing

  import Ecto.Query

  alias EctoShorts.Dynamics
  alias EctoShorts.Schema.Post

  test "convert_to_dynamic supports boolean operator :or" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{or: [published: true, published: false]})

    expected =
      dynamic(
        [q],
        field(q, ^:published) == ^true or field(q, ^:published) == ^false
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports composite boolean :or expressions" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        or: [
          %{id: 1, published: true},
          %{id: 2, published: false}
        ]
      })

    expected =
      dynamic(
        [q],
        (field(q, ^:id) == ^1 and field(q, ^:published) == ^true) or
          (field(q, ^:id) == ^2 and field(q, ^:published) == ^false)
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports boolean operator :and" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{and: [published: true, title: "hi"]})

    expected =
      dynamic(
        [q],
        field(q, ^:published) == ^true and field(q, ^:title) == ^"hi"
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports boolean operator :or with map value" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, %{or: [published: true, title: "hi"]})

    expected =
      dynamic(
        [q],
        field(q, ^:published) == ^true or field(q, ^:title) == ^"hi"
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic preserves struct values (DateTime) for scalar comparisons" do
    binding = {:as, nil}
    dt = ~U[2026-01-01 00:00:00Z]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{published_at: dt})

    expected =
      dynamic(
        [q],
        field(q, ^:published_at) == ^dt
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic preserves struct values (DateTime) for operator map comparisons" do
    binding = {:as, nil}
    dt = ~U[2026-01-01 00:00:00Z]
    actual = Dynamics.convert_to_dynamic(Post, binding, %{published_at: %{>=: dt}})

    expected =
      dynamic(
        [q],
        field(q, ^:published_at) >= ^dt
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports avg helper expression on a field from keyword params" do
    binding = {:as, nil}
    actual = Dynamics.convert_to_dynamic(Post, binding, views: %{avg: %{>: 10}})
    expected = dynamic([q], avg(field(q, ^:views)) > ^10)

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic passes exists payload through to adapter without subquery resolution" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(
        Post,
        binding,
        %{exists: %{from: Post, id: 1}},
        dynamic_adapter: EctoShorts.TestPayloadProbeAdapter
      )

    assert match?(%Ecto.Query.DynamicExpr{}, actual)
    assert_received {:payload_probe_expr, :exists, %{from: Post, id: 1}}
  end

  test "convert_to_dynamic passes all/any payload wrappers through to adapter without subquery resolution" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(
        Post,
        binding,
        %{id: %{all: %{>: %{from: Post, id: 1}}}},
        dynamic_adapter: EctoShorts.TestPayloadProbeAdapter
      )

    assert match?(%Ecto.Query.DynamicExpr{}, actual)
    assert_received {:payload_probe_expr, :id, %{all: %{>: %{from: Post, id: 1}}}}
  end

  test "convert_to_dynamic supports datetime: add helper map payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        inserted_at: %{>=: %{datetime: %{add: %{field: :inserted_at, count: 1, interval: "day"}}}}
      })

    expected =
      dynamic(
        [q],
        field(q, ^:inserted_at) >= datetime_add(field(q, ^:inserted_at), ^1, ^"day")
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports date: add helper map payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        inserted_at: %{==: %{date: %{add: %{field: :inserted_at, count: 1, interval: "month"}}}}
      })

    expected =
      dynamic(
        [q],
        field(q, ^:inserted_at) == date_add(field(q, ^:inserted_at), ^1, ^"month")
      )

    assert_dynamic(expected, actual)
  end

  test "convert_to_dynamic supports datetime: from_now helper map payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        inserted_at: %{<: %{datetime: %{from_now: %{count: 2, interval: "week"}}}}
      })

    actual_ast = Macro.to_string(actual)

    assert actual_ast =~ "q.inserted_at < datetime_add("
    assert actual_ast =~ "^2, \"week\""
  end

  test "convert_to_dynamic supports datetime: ago helper map payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        inserted_at: %{>: %{datetime: %{ago: %{count: 7, interval: "day"}}}}
      })

    actual_ast = Macro.to_string(actual)

    assert actual_ast =~ "q.inserted_at > datetime_add("
    assert actual_ast =~ "^-7, \"day\""
  end

  test "convert_to_dynamic supports implicit equality for date/time helper map payload" do
    binding = {:as, nil}

    actual =
      Dynamics.convert_to_dynamic(Post, binding, %{
        inserted_at: %{datetime: %{from_now: %{count: 1, interval: "day"}}}
      })

    actual_ast = Macro.to_string(actual)

    assert actual_ast =~ "q.inserted_at == datetime_add("
    assert actual_ast =~ "^1, \"day\""
  end
end
