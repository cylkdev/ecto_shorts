# Non-assertive Pattern Matching

**Problem**

Overall, Elixir systems are composed of many supervised processes, so the effects of an error are localized to a single process, and don't propagate to the entire application. A supervisor detects the failing process, reports it, and possibly restarts it. This anti-pattern arises when developers write defensive or imprecise code, capable of returning incorrect values which were not planned for, instead of applicationming in an assertive style through pattern matching and guards.

**Example**

The function `get_value/2` tries to extract a value from a specific key of a URL query string. As it is not implemented using pattern matching, `get_value/2` always returns a value, regardless of the format of the URL query string passed as a parameter in the call. Sometimes the returned value will be valid. However, if a URL query string with an unexpected format is used in the call, `get_value/2` will extract incorrect values from it:

```elixir
defmodule Extract do
  def get_value(string, desired_key) do
    parts = String.split(string, "&")

    Enum.find_value(parts, fn pair ->
      key_value = String.split(pair, "=")
      Enum.at(key_value, 0) === desired_key && Enum.at(key_value, 1)
    end)
  end
end
```

```elixir
# URL query string with the planned format - OK!
Extract.get_value("name=Lucas&university=UFMG&lab=ASERG", "lab")
"ASERG"
Extract.get_value("name=Lucas&university=UFMG&lab=ASERG", "university")
"UFMG"
# Unplanned URL query string format - Unplanned value extraction!
Extract.get_value("name=Lucas&university=institution=UFMG&lab=ASERG", "university")
"institution"   # <= why not "institution=UFMG"? or only "UFMG"?
```

**Refactoring**

To remove this anti-pattern, `get_value/2` can be refactored through the use of pattern matching. So, if an unexpected URL query string format is used, the function will crash instead of returning an invalid value. This behavior, shown below, allows clients to decide how to handle these errors and doesn't give a false impression that the code is working correctly when unexpected values are extracted:

```elixir
defmodule Extract do
  def get_value(string, desired_key) do
    parts = String.split(string, "&")

    Enum.find_value(parts, fn pair ->
      [key, value] = String.split(pair, "=") # <= pattern matching
      key === desired_key && value
    end)
  end
end
```

```elixir
# URL query string with the planned format - OK!
Extract.get_value("name=Lucas&university=UFMG&lab=ASERG", "name")
"Lucas"
# Unplanned URL query string format - Crash explaining the problem to the client!
Extract.get_value("name=Lucas&university=institution=UFMG&lab=ASERG", "university")
** (MatchError) no match of right hand side value: ["university", "institution", "UFMG"]
  extract.ex:7: anonymous fn/2 in Extract.get_value/2 # <= left hand: [key, value] pair
Extract.get_value("name=Lucas&university&lab=ASERG", "university")
** (MatchError) no match of right hand side value: ["university"]
  extract.ex:7: anonymous fn/2 in Extract.get_value/2 # <= left hand: [key, value] pair
```

Elixir and pattern matching promote an assertive style of applicationming where you handle the known cases. Once an unexpected scenario arises, you can decide to address it accordingly based on practical examples, or conclude the scenario is indeed invalid and the exception is the desired choice.

`case/2` is another important construct in Elixir that help us write assertive code, by matching on specific patterns. For example, if a function returns `{:ok, ...}` or `{:error, ...}`, prefer to explicitly match on both patterns:

```elixir
case some_function(arg) do
  {:ok, value} -> # ...
  {:error, _} -> # ...
end
```

In particular, avoid matching solely on `_`, as shown below:

```elixir
case some_function(arg) do
  {:ok, value} -> # ...
  _ -> # ...
end
```

Matching on `_` is less clear in intent and it may hide bugs if `some_function/1` adds new return values in the future.
