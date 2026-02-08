## Summary

The feedback received requires changes to the current implementation plan `[Plan Name]`. These modifications must be reviewed and approved before proceeding.

## Feedback Summary

- Restate the feedback in one sentence. If you can't, ask for clarification.

- In at most 3 sentences, explain why the feedback impacts the implementation plan.

- If the feedback is related to another task, include the following:
  - Related to: `[Task name]`

## Current Implementation

The current approach implements [describe current behavior].

```elixir
defmodule MyApp.CurrentImplementation do
  @moduledoc """
  Current implementation handles data processing.
  """

  def process_data(string, match, replacement) 
      when is_binary(string) and is_binary(match) and is_binary(replacement) do
    do_replace(string, match, replacement)
  end

  defp do_replace(string, "", _replacement) do
    # Current: Returns string unchanged
    string
  end

  defp do_replace(string, match, replacement) do
    # Implementation logic
    String.replace(string, match, replacement)
  end
end
```

### Current Behavior

- Allows empty string as match parameter
- Returns original string when match is empty
- No validation or error handling for edge cases

### Proposed Changes

Based on feedback, the implementation needs to:

- Add validation for empty string matches
- Raise explicit errors when invalid inputs are provided
- Update documentation to clarify behavior with edge cases

### Impacted Areas

- **Modules affected:** `MyApp.DataProcessor`, `MyApp.StringHelpers`
- **Functions modified:** `process_data/3`, `validate_input/1`
- **Breaking changes:** Yes - will raise `ArgumentError` for previously accepted empty strings
- **Migration path:** Users must check for empty strings before calling

### Implementation Flow Changes

**File:** `lib/my_app/data_processor.ex`
**Module/Function:** `MyApp.DataProcessor.process_data/3`

**BEFORE:**

```
      MyApp.DataProcessor.process_data("hello", "", "world")
                     |
                     |
            +--------+--------+
            |                 |
        Accept All        No Validation
            |                 |
            +--------+--------+
                     |
                Process Empty
                     |
                Return "hello"
                (unchanged)            
```

- User calls `process_data("hello", "", "world")`
- Function accepts all parameters without validation
- Internal logic attempts to process empty match
- Returns original string unchanged (implicit behavior)
- No indication to user that input was problematic

**AFTER:**

```
      MyApp.DataProcessor.process_data("hello", "", "world")
                     |
                     |
              Validate Match
                     |
            +--------+--------+
            |                 |
        match == ""       match valid
            |                 |
            v                 v
        Raise Error         Process Data
        ArgumentError         |
                              v
                        Return Result
```

- User calls `process_data("hello", "", "world")`
- Function validates match parameter
- If match is empty string → raise `ArgumentError` with clear message
- If match is valid → proceed with processing
- User receives explicit feedback about invalid input

### Data Flow Changes

**File:** `lib/my_app/data_processor.ex`
**Module/Function:** `MyApp.DataProcessor.process_data/3`

**BEFORE:**

```
User Input -> Function Entry -> Pattern Match -> Return
   "hello"         |                  |            |
   ""              |                  |            |
   "world"         v                  v            v
              No checks         Empty clause   "hello"
              performed         matches        (unchanged)
```

**AFTER:**

```
User Input -> Function Entry -> Validation -> Processing -> Return
   "hello"         |                |              |           |
   ""              |                v              |           |
   "world"         v          match == "" ?        |           |
              Guards OK            |               |           |
                              +----+----+          |           |
                          YES |         | NO       |           |
                              v         v          v           v
                          Raise      Continue  Execute    Result
                          Error      ->        Logic      to User
```

### Code Changes

**File:** `lib/my_app/data_processor.ex`
**Module/Function:** `MyApp.DataProcessor`

**BEFORE:**

```elixir
defmodule MyApp.DataProcessor do
  @doc """
  Processes data by replacing matches.

  ## Examples

      iex> MyApp.DataProcessor.process_data("hello", "l", "r")
      "herro"

  """
  @spec process_data(String.t(), String.t(), String.t()) :: String.t()
  def process_data(string, match, replacement) 
      when is_binary(string) and is_binary(match) and is_binary(replacement) do
    do_process(string, match, replacement)
  end

  defp do_process(string, "", _replacement) do
    string
  end

  defp do_process(string, match, replacement) do
    # Processing logic
    result = String.replace(string, match, replacement)
    result
  end
end
```

**AFTER:**

```elixir
defmodule MyApp.DataProcessor do
  @doc """
  Processes data by replacing matches.

  Raises `ArgumentError` if `match` is an empty string, as it's impossible
  to replace multiple occurrences of an empty string meaningfully.

  ## Examples

      iex> MyApp.DataProcessor.process_data("hello", "l", "r")
      "herro"

      iex> MyApp.DataProcessor.process_data("hello", "", "x")
      ** (ArgumentError) cannot use an empty string as the match to replace

  """
  @spec process_data(String.t(), String.t(), String.t()) :: String.t() | no_return()
  def process_data(string, match, replacement) 
      when is_binary(string) and is_binary(match) and is_binary(replacement) do
    if match == "" do
      raise ArgumentError, "cannot use an empty string as the match to replace"
    end
    
    do_process(string, match, replacement)
  end

  defp do_process(string, match, replacement) do
    # Processing logic
    result = String.replace(string, match, replacement)
    result
  end
end
```

**Key differences:**

- Line `4-5`: Added documentation about `ArgumentError` for empty strings
- Line `11-13`: Added explicit examples showing the error case
- Line `16`: Updated typespec to include `no_return()` for error cases
- Line `19-21`: Added validation check that raises descriptive error
- Removed: `do_process/3` clause for empty string (no longer needed)

---

**File:** `test/my_app/data_processor_test.exs`

**BEFORE:**

```elixir
defmodule MyApp.DataProcessorTest do
  use ExUnit.Case
  alias MyApp.DataProcessor

  test "process_data/3 replaces matches" do
    assert DataProcessor.process_data("hello", "l", "r") == "herro"
  end

  test "process_data/3 with empty match returns original" do
    assert DataProcessor.process_data("hello", "", "x") == "hello"
  end
end
```

**AFTER:**

```elixir
defmodule MyApp.DataProcessorTest do
  use ExUnit.Case
  alias MyApp.DataProcessor

  describe "process_data/3" do
    test "replaces matches correctly" do
      assert DataProcessor.process_data("hello", "l", "r") == "herro"
    end

    test "raises ArgumentError when match is empty string" do
      assert_raise ArgumentError, 
                   "cannot use an empty string as the match to replace",
                   fn ->
                     DataProcessor.process_data("hello", "", "x")
                   end
    end

    test "raises ArgumentError when match is empty even if string is empty" do
      assert_raise ArgumentError,
                   "cannot use an empty string as the match to replace",
                   fn ->
                     DataProcessor.process_data("", "", "found")
                   end
    end
  end
end
```

**Test changes:**

- Replaced test with describe block for better organization
- Changed empty string test from expecting return value to expecting ArgumentError
- Added additional edge case test for empty string with empty input   

**Migration Guide**

<instruction>

- Only include the Migration Guide section if this is a public API change.

</instruction>

For existing code using this function:

```elixir
# Old code (will now raise)
result = MyApp.DataProcessor.process_data(text, pattern, replacement)

# New code (add validation)
if pattern != "" do
  result = MyApp.DataProcessor.process_data(text, pattern, replacement)
else
  # Handle empty pattern case
  result = text  # or raise your own error
end
```

## Rationale

<instruction>
Explain your reasoning in simple non-technical words as if the audience has no prior knowledge of the code.
</instruction>

This change addresses the fundamental issue that processing empty string matches leads to infinite recursion or undefined behavior. As demonstrated in the property testing discussion:

```elixir
# This property fails with timeout when ls is empty string:
property "replace all multiple occurrences of string" do
  forall {ls, n, rs} <- {string(), nat(), string()} do
    string = String.duplicate(ls, n)
    replaced = String.duplicate(rs, n)
    ensure String.replace_leading(string, ls, rs) == replaced
  end
end
```

After all, how would you replace 4 copies of "" by 4 copies of "foo"? The number of occurrences is undefined when string length is zero.
