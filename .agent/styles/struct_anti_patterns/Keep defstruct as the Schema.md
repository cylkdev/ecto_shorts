# Keep defstruct as the Schema

When you define a struct in Elixir, use `defstruct` to declare the **shape** of the struct (the field names), and keep the **default values** in a separate place that is used when you build the struct.

Do not put runtime or configurable defaults directly inside `defstruct` when the module also has a constructor like `new/1`. This makes the source of truth unclear because some defaults live in the struct definition and some may later move into `new/1`. A novice reading the module should be able to answer two questions immediately: "What fields exist?" and "Where do default values come from?" The answer should be consistent every time.

The rule is simple: `defstruct` lists fields only, and `new/1` applies defaults from a dedicated default options source. That default source can be a module attribute when all defaults are static, or a private function when one or more defaults must be computed at runtime.

This gives the same outcome every time because the struct shape is always in one place and the defaults are always merged in one place. It also prevents accidental differences between the struct defaults and the constructor defaults.

A struct serves two jobs if you put values directly in `defstruct`: it defines the fields and it defines defaults. That is fine for very small modules, but it becomes harder to maintain when you also expose `new/1` and want to merge user options. In that case, separating field names from defaults makes the module easier to read and safer to change.

It also makes runtime defaults explicit. For example, `System.schedulers_online()` depends on the runtime environment. A novice should be able to see that this value is computed when building defaults, not treated like a fixed constant.

### Bad example

Do not do this:

    defmodule BigmemStore.PartitionPolicy do
      @moduledoc false

      defstruct partitions: 2,
                pool_size: System.schedulers_online(),
                max_items: 10_000,
                max_bytes: :infinity,
                table_type: :ordered_set

      def new(opts \\ []) when is_list(opts) do
        struct(__MODULE__, opts)
      end
    end

### Good Examples

Instead, you should do this:

    defmodule BigmemStore.PartitionPolicy do
      @moduledoc false

      defstruct [
        :partitions,
        :pool_size,
        :max_items,
        :max_bytes,
        :table_type
      ]

      @default_options [
        partitions: 2,
        pool_size: 2,
        max_items: 10_000,
        max_bytes: :infinity,
        table_type: :ordered_set
      ]

      def new(opts \\ []) when is_list(opts) do
        struct(__MODULE__, Keyword.merge(@default_options, opts))
      end
    end

or this:

    defmodule BigmemStore.PartitionPolicy do
      @moduledoc false

      defstruct [
        :partitions,
        :pool_size,
        :max_items,
        :max_bytes,
        :table_type
      ]

      @default_options [
        partitions: 2,
        max_items: 10_000,
        max_bytes: :infinity,
        table_type: :ordered_set
      ]

      def new(opts \\ []) when is_list(opts) do
        struct(__MODULE__, Keyword.merge(default_options(), opts))
      end

      defp default_options do
        Keyword.put(@default_options, :pool_size, System.schedulers_online())
      end
    end
