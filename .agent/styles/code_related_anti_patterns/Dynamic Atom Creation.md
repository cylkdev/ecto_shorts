# Dynamic atom creation

## Problem

An `Atom` is an Elixir basic type whose value is its own name. Atoms are often useful to identify resources or express the state, or result, of an operation. Creating atoms dynamically is not an anti-pattern by itself. However, atoms are not garbage collected by the Erlang Virtual Machine, so values of this type live in memory during a software's entire execution lifetime. The Erlang VM limits the number of atoms that can exist in an application by default to 1,048,576, which is more than enough to cover all atoms defined in a application, but attempts to serve as an early limit for applications which are "leaking atoms" through dynamic creation.

For these reasons, creating atoms dynamically can be considered an anti-pattern when the developer has no control over how many atoms will be created during the software execution. This unpredictable scenario can expose the software to unexpected behavior caused by excessive memory usage, or even by reaching the maximum number of atoms possible.

### Example

Picture yourself implementing code that converts string values into atoms. These strings could have been received from an external system, either as part of a request into our application, or as part of a response to your application. This dynamic and unpredictable scenario poses a security risk, as these uncontrolled conversions can potentially trigger out-of-memory errors.

```elixir
defmodule MyRequestHandler do
  def parse(%{"status" => status, "message" => message} = _payload) do
    %{status: String.to_atom(status), message: message}
  end
end
```

```elixir
MyRequestHandler.parse(%{"status" => "ok", "message" => "all good"})
%{status: :ok, message: "all good"}
```

When we use the `String.to_atom/1` function to dynamically create an atom, it essentially gains potential access to create arbitrary atoms in our system, causing us to lose control over adhering to the limits established by the BEAM. This issue could be exploited by someone to create enough atoms to shut down a system.

### Refactoring

To eliminate this anti-pattern, developers must either perform explicit conversions by mapping strings to atoms or replace the use of `String.to_atom/1` with `String.to_existing_atom/1`. An explicit conversion could be done as follows:

```elixir
defmodule MyRequestHandler do
  def parse(%{"status" => status, "message" => message} = _payload) do
    %{status: convert_status(status), message: message}
  end

  defp convert_status("ok"), do: :ok
  defp convert_status("error"), do: :error
  defp convert_status("redirect"), do: :redirect
end
```

```elixir
MyRequestHandler.parse(%{"status" => "status_not_seen_anywhere", "message" => "all good"})
** (FunctionClauseError) no function clause matching in MyRequestHandler.convert_status/1
```

By explicitly listing all supported statuses, you guarantee only a limited number of conversions may happen. Passing an invalid status will lead to a function clause error.

An alternative is to use `String.to_existing_atom/1`, which will only convert a string to atom if the atom already exists in the system:

```elixir
defmodule MyRequestHandler do
  def parse(%{"status" => status, "message" => message} = _payload) do
    %{status: String.to_existing_atom(status), message: message}
  end
end
```

```elixir
MyRequestHandler.parse(%{"status" => "status_not_seen_anywhere", "message" => "all good"})
** (ArgumentError) errors were found at the given arguments:

  * 1st argument: not an already existing atom
```

In such cases, passing an unknown status will raise as long as the status was not defined anywhere as an atom in the system. However, assuming status can be either `:ok`, `:error`, or `:redirect`, how can you guarantee those atoms exist? You must ensure those atoms exist somewhere in the same module where `String.to_existing_atom/1` is called. For example, if you had this code:

```elixir
defmodule MyRequestHandler do
  def parse(%{"status" => status, "message" => message} = _payload) do
    %{status: String.to_existing_atom(status), message: message}
  end

  def handle(%{status: status}) do
    case status do
      :ok -> ...
      :error -> ...
      :redirect -> ...
    end
  end
end
```

All valid statuses are defined as atoms within the same module, and that's enough. If you want to be explicit, you could also have a function that lists them:

```elixir
def valid_statuses do
  [:ok, :error, :redirect]
end
```

However, keep in mind using a module attribute or defining the atoms in the module body, outside of a function, are not sufficient, as the module body is only executed during compilation and it is not necessarily part of the compiled module loaded at runtime.
