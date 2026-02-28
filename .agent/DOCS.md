# Documentation

This is the documentation standard for this project.

Follow it so a beginner can understand the public API, call it correctly on the first try, predict every user-observable behavior, and verify that behavior using examples.

## What counts as “documentation” in Elixir

Elixir documentation is Markdown attached to code.

Use `@moduledoc` for modules.
Use `@doc` for public functions, macros, and callbacks.
Use `@typedoc` for public types.
Use `@spec` to define the input and output shapes.

Readers will treat these docs as the source of truth, so they must be complete and accurate.

## Core rules

Write docs as interface descriptions, not as implementation walkthroughs.
Describe only what a caller can observe at the boundary.
Include everything a caller can observe, even if it is not the return value.

Keep the docs self-contained.
A beginner must be able to succeed using only the doc on the module or function they are reading.

Use plain language.
Define non-obvious terms the first time you use them.

Make every sentence unambiguous.
Each sentence must have only one reasonable interpretation.

Keep vocabulary consistent across the repo.
Use the same term for the same concept everywhere.

Start with the summary.
The first line must tell the reader what this module or function does.

## User-observable behavior

“User-observable” means anything a caller can see happen at the boundary.
This includes return values, raised errors, logs, messages to other processes, database writes, files created, telemetry emitted, retries, timeouts, ordering, and concurrency effects.

If a caller could notice it, document it.

## How to write module docs

Start with a one-line summary.
Write one plain sentence that starts with a verb.

After the summary, answer these questions in prose.
Do not add extra section headers for them.

Explain when to use the module.
State the situations where a caller should reach for it.
State the situations where a caller should not use it.

Explain what the module owns.
Describe the responsibilities a caller can rely on.
List important non-goals when they prevent common mistakes.

Name the public entry points.
Point a new reader to the functions they should start with.
For each entry point, describe what it does and what it returns.

Describe the observable contract.
Explain required inputs and accepted shapes.
Explain outputs and their exact shapes.
Explain error cases and what the caller observes.
Explain side effects and when they happen.
Explain ordering, concurrency, retries, and timeouts when they matter.
Mention surprising costs only when a caller should care.

Keep module docs short, but never omit observable behavior.

## How to write function docs

Start with a one-line summary that starts with a verb.

Then describe the contract in prose.
State what each argument means in plain language.
If the function accepts more than one shape, list each accepted shape.

Describe the return value exactly.
Prefer concrete shapes like `{:ok, value}` and `{:error, reason}`.
If the function can raise, say what triggers the exception and what the caller will observe.

List side effects.
Mention logs, database writes, messages, telemetry, file IO, network calls, and any observable timing behavior.

Only describe performance when it changes how a caller should use the function.

## Options

Use an `## Options` section only when the function takes options.
List each option key, its default, and the behavior it changes.
Describe options in terms of what the caller can observe.

Example format:

    ## Options

    * `:timeout` (default: `5_000`) sets how long the call waits before returning `{:error, :timeout}`.
    * `:mode` (default: `:safe`) selects validation rules that affect which inputs return `{:error, reason}`.

## Examples

Use an `## Examples` section for copy/paste-able examples.
Prefer IEx-style examples.

    ## Examples

        iex> MyApp.Payments.charge(order, [])
        {:ok, receipt}

Include a happy path and at least one realistic edge case.
If output varies (time, randomness, network), say what varies and what stays stable.

## Multi-clause functions

Put `@doc` above the first clause.
If argument names inferred by Elixir would be confusing, add a function head to control names.

    def size(map_with_size)

    def size(%{size: size}) do
      size
    end

## OTP modules

If a module starts or supervises processes, document startup and crash behavior in caller terms.
Describe how to start it.
Describe what a successful start means and how a caller can verify it.
Describe what happens on crashes and restarts in observable terms, including downtime and duplicated work when relevant.

Keep this description in prose.
Use `## Examples` for a start-and-use snippet.

## Templates

Use these as starting points.
Delete anything that does not apply, but do not delete observable behavior.

Module template:

    defmodule MyApp.Example do
      @moduledoc """
      Does one clear thing in one sentence.

      Use this module when (describe the situation).
      Do not use this module when (describe the boundary).

      This module is responsible for (describe caller-visible responsibilities).
      This module is not responsible for (list important non-goals).

      Start with `new/1` when you need (one-line outcome).
      Call `run/2` to (one-line outcome).

      Inputs are (describe required shapes and required keys).
      On success it returns (exact shape).
      On failure it returns (exact shape) or raises (exception and trigger).
      It also (list side effects a caller can observe).

      ## Examples

          iex> MyApp.Example.new(%{})
          {:ok, value}
      """
    end

Function template:

    @doc """
    Does one thing in one sentence.

    `arg1` is (plain meaning and accepted shapes).
    `arg2` is (plain meaning and accepted shapes).

    On success it returns (exact shape).
    On failure it returns (exact shape).
    It raises (exception) when (trigger), and the caller observes (what happens).

    It also (side effects).

    ## Options

    * `:timeout` (default: `5_000`) (observable effect).

    ## Examples

        iex> MyApp.Example.run(arg1, arg2)
        {:ok, value}
    """
    @spec run(arg1_type, arg2_type) :: return_type
    def run(arg1, arg2) do
      ...
    end

## Done means “fully described”

Consider docs done only when a beginner can answer these from the doc they are reading.
They can explain when to use it.
They can provide valid inputs without guessing shapes or required keys.
They can predict return values and error shapes.
They can predict side effects.
They can predict relevant ordering, concurrency, retries, and timeouts.
They can verify behavior using the examples.

If any user-observable behavior is missing, the docs are not done.