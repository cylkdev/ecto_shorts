defmodule EctoShorts.Testing do
  @doc """
  Asserts that two dynamic expressions are identical.
  """
  defmacro assert_dynamic(dyn_a, dyn_b) do
    quote do
      assert Macro.to_string(unquote(dyn_a)) === Macro.to_string(unquote(dyn_b))
    end
  end
end
