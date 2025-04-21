# defmodule EctoShorts.CommonFilters.CommonQueryBuildersApi.Dynamic do
#   @doc """
#   ...
#   """
#   def traverse_params(dyn \\ nil, current_binding \\ nil, params, fun)

#   def traverse_params(dyn, current_binding, {schema_field, {operator, value}}, fun) do
#     fun.(dyn, current_binding, schema_field, operator, value)
#   end

#   def traverse_params(dyn, current_binding, {schema_field, values}, fun) when is_list(values) do
#     if Keyword.keyword?(values) do
#       Enum.reduce(values, dyn, fn {key, value}, dyn ->
#         traverse_params(dyn, current_binding, {schema_field, {key, value}}, fun)
#       end)
#     else
#       fun.(dyn, current_binding, schema_field, :==, values)
#     end
#   end

#   def traverse_params(dyn, current_binding, {schema_field, params}, fun) when is_map(params) do
#     Enum.reduce(params, dyn, fn {key, value}, dyn ->
#       traverse_params(dyn, current_binding, {schema_field, {key, value}}, fun)
#     end)
#   end

#   def traverse_params(dyn, current_binding, {schema_field, value}, fun) do
#     fun.(dyn, current_binding, schema_field, :==, value)
#   end

#   def traverse_params(dyn, current_binding, params, fun) do
#     Enum.reduce(params, dyn, fn {key, value}, dyn ->
#       traverse_params(dyn, current_binding, {key, value}, fun)
#     end)
#   end
# end
