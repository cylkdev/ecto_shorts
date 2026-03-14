# Let the data shape drive the code

Express behavior from the structure of the data itself. Do not accept a broad input and then inspect, classify, and branch on it inside the function body. Encode the distinction at the boundary of the function, in the function head, in the recursion, or in the data model.

Write code that follows the shape of the input. Do not write code that first asks what the input is and then decides what to do with it. In practice, prefer structural decomposition over procedural inspection.

This is the underlying rule. Multiple function clauses are one way to apply it. They are not the rule itself.

In Elixir, this means the function should reflect the forms it handles. Empty lists, non-empty lists, tuples, and fallbacks should appear as direct structure in the implementation. Avoid broad guards and nested branching when the data already provides the distinction.

### Do not do this

    defp normalize_all_payload(payload) when is_list(payload) do
      if Keyword.keyword?(payload) do
        case payload do
          [{op, value}] -> {normalize_operator(op), value}
          _ -> payload
        end
      else
        payload
      end
    end

This code receives a general value, then interrogates it step by step to discover its shape.

### Do this

    defp normalize_all_payload([]), do: []

    defp normalize_all_payload([head | tail]) do
      [normalize_all_payload(head) | normalize_all_payload(tail)]
    end

    defp normalize_all_payload({op, value}) do
      {normalize_operator(op), value}
    end

    defp normalize_all_payload(payload) do
      payload
    end

This code follows the structure of the input directly. The control flow emerges from the data shape instead of being layered on afterward.

A concise style-guide version is this:

Match on structure. Do not inspect a broad value and then branch to recover distinctions the language can express directly.

A slightly stronger version is this:

Make the code mirror the data. When behavior varies by shape, encode that variation where the value enters the function, not as nested decision logic inside it.

If you want, I can turn this into a one-paragraph rule written in the style of an internal engineering handbook.
