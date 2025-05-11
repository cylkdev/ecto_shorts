defmodule EctoShorts.Testing do
  defmacro assert_dynamic(dyn_a, dyn_b) do
    quote do
      dyn_a = Macro.to_string(unquote(dyn_a))
      dyn_b = Macro.to_string(unquote(dyn_b))

      assert dyn_a === dyn_b
    end
  end
end
