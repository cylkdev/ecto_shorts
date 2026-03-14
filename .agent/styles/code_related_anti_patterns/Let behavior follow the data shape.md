---
description: Match on structure. Make behavior visible at the function boundary. Do not inspect a broad value to recover distinctions Elixir can express directly, and do not replace that mistake with patterns that are so specific they only work for one exact recursive shape.
---

# Let behavior follow the data shape

In Elixir, write functions so the shapes that matter to behavior are explicit at the boundary. Pattern match on meaningful forms in function heads, recursive steps, and fallback clauses so the exact outcome is visible from the structure of the code. A human or coding agent should be able to read the implementation and reason from input shape to result without first tracing a chain of internal checks.

Do not accept a broad value and then inspect, classify, and branch on it to recover distinctions Elixir can already express directly. If behavior differs for an empty list, a non-empty list, a tuple, or a fallback value, make those cases structural. Prefer decomposition over interrogation.

Just as important: do not confuse “pattern matching” with “hard-coding one exact input.” A pattern like `[{op, value}]` does not describe recursive structure. It describes one very specific list shape: a list with exactly one two-element tuple. If a caller passes anything else, that clause stops applying, and in recursive code that usually means brittle behavior, missed cases, or a function clause crash somewhere in the traversal.

This is the rule. Multiple function clauses are one common tool, but they are not the point by themselves. The point is to make the implementation mirror the cases the function actually handles, so behavior is easy to predict, safe across valid inputs, and straightforward to change.

In practice:

- Match on the input shapes that change behavior.
- Let recursion reflect how nested data is processed.
- Use guards to refine a known shape, not to rediscover one.
- Keep fallback clauses explicit so default behavior is obvious.
- Avoid overly specific patterns unless that exact shape is the contract.
- Put branching inside the body only when the decision depends on computed state, not on structure already present in the input.

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

This code starts with a broad category, then inspects the value step by step to discover what it should do. It also hard-codes one exact list shape inside the `case`. That makes the behavior harder to reason about and easy to break when the recursive input is slightly different from the one special case the code was written around.

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

This code makes the behavior structural. Each clause names a meaningful form of input, and the outcome follows directly from that form. The recursion handles lists by their natural shape, not by guessing whether the whole list happens to match one narrow pattern. That makes the function easier to extend and less likely to fail when the input varies within the shapes the function is meant to support.

A useful test is this: if someone reads only the function heads, can they predict the exact outcome for each meaningful case, and can they see which inputs safely fall through to a default? If not, the code is probably hiding behavior that should be expressed through pattern matching, recursion, or a clearer data model.
