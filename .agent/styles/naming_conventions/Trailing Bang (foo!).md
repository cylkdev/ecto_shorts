# Trailing Bang (foo!)

A trailing bang (exclamation mark) signifies a function or macro where failure cases raise an exception. They most often exist as a "raising variant" of a function that returns :ok/:error tuples (or nil).

One example is `File.read/1` and `File.read!/1`. `File.read/1` will return a success or failure tuple, whereas `File.read!/1` will return a plain value or else raise an exception:

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

```elixir
File.read("file.txt")
{:ok, "file contents"}
File.read("no_such_file.txt")
{:error, :enoent}

File.read!("file.txt")
"file contents"
File.read!("no_such_file.txt")
** (File.Error) could not read file no_such_file.txt: no such file or directory
```

The version without ! is preferred when you want to handle different outcomes using pattern matching:

```elixir
case File.read(file) do
  {:ok, body} -> # do something with the `body`
  {:error, reason} -> # handle the error caused by `reason`
end
```

However, if you expect the outcome to always be successful (for instance, if you expect the file always to exist), the bang variation can be more convenient and will raise a more helpful error message (than a failed pattern match) on failure.

When thinking about failure cases, we are often thinking about semantic errors related to the operation being performed, such as failing to open a file or trying to fetch key from a map. Errors that come from invalid argument types, or similar, must always raise regardless if the function has a bang or not. In such cases, the exception is often an `ArgumentError` or a detailed `FunctionClauseError`:

```elixir
File.read(123)
** (FunctionClauseError) no function clause matching in IO.chardata_to_string/1

    The following arguments were given to IO.chardata_to_string/1:

        # 1
        123

    Attempted function clauses (showing 2 out of 2):

        def chardata_to_string(string) when is_binary(string)
        def chardata_to_string(list) when is_list(list)
```

More examples of paired functions: `Base.decode16/2` and `Base.decode16!/2`, `File.cwd/0` and `File.cwd!/0`. In some situations, you may have bang functions without a non-bang counterpart. They also imply the possibility of errors, such as: `Protocol.assert_protocol!/1` and `PartitionSupervisor.resize!/2`. This can be useful if you foresee the possibility of adding a non-raising variant in the future.
