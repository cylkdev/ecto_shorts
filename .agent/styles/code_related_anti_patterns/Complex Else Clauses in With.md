# Complex else clauses in with

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

## Problem

This anti-pattern refers to with expressions that flatten all its error clauses into a single complex else block. This situation is harmful to the code readability and maintainability because it's difficult to know from which clause the error value came.

### Example

An example of this anti-pattern, as shown below, is a function `open_decoded_file/1` that reads a Base64-encoded string content from a file and returns a decoded binary string. This function uses a with expression that needs to handle two possible errors, all of which are concentrated in a single complex else block.

```elixir
def open_decoded_file(path) do
  with {:ok, encoded} <- File.read(path),
       {:ok, decoded} <- Base.decode64(encoded) do
    {:ok, String.trim(decoded)}
  else
    {:error, _} -> {:error, :badfile}
    :error -> {:error, :badencoding}
  end
end
```

In the code above, it is unclear how each pattern on the left side of `<-` relates to their error at the end. The more patterns in a with, the less clear the code gets, and the more likely it is that unrelated failures will overlap each other.

### Solution

In this situation, instead of concentrating all error handling within a single complex else block, it is better to normalize the return types in specific private functions. In this way, with can focus on the success case and the errors are normalized closer to where they happen, leading to better organized and maintainable code.

```elixir
def open_decoded_file(path) do
  with {:ok, encoded} <- file_read(path),
       {:ok, decoded} <- base_decode64(encoded) do
    {:ok, String.trim(decoded)}
  end
end

defp file_read(path) do
  case File.read(path) do
    {:ok, contents} -> {:ok, contents}
    {:error, _} -> {:error, :badfile}
  end
end

defp base_decode64(contents) do
  case Base.decode64(contents) do
    {:ok, decoded} -> {:ok, decoded}
    :error -> {:error, :badencoding}
  end
end
```
