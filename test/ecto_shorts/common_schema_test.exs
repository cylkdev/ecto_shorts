# defmodule EctoShorts.CommonSchemaTest do
#   use ExUnit.Case, async: true
#   doctest EctoShorts.CommonSchema

#   alias EctoShorts.CommonSchema

#   require Ecto.Query

#   describe "reflection_for_schema/1: " do
#     test "returns expected value given schema" do
#       assert [:id, :body, :inserted_at, :updated_at] =
#                EctoShorts.CommonSchema.reflection_for_schema(EctoShorts.Schema.Post, :fields)
#     end

#     test "returns expected value given {schema_source, schema_module}" do
#       assert [:id, :body, :inserted_at, :updated_at] =
#                EctoShorts.CommonSchema.reflection_for_schema(
#                  {"concrete_table", AbstractSchema},
#                  :fields
#                )
#     end
#   end

#   describe "reflection_for_schema/2: " do
#     test "returns expected value given schema" do
#       assert :string =
#                EctoShorts.CommonSchema.reflection_for_schema(EctoShorts.Schema.Post, :type, :body)
#     end

#     test "returns expected value given {schema_source, schema_module}" do
#       assert :string =
#                EctoShorts.CommonSchema.reflection_for_schema(
#                  {"concrete_table", AbstractSchema},
#                  :type,
#                  :body
#                )
#     end
#   end

#   describe "get_loaded_struct/2: " do
#     test "returns struct with loaded state and source when given a queryable" do
#       assert %EctoShorts.Schema.Post{
#                __meta__: %Ecto.Schema.Metadata{
#                  state: :loaded,
#                  source: "basic_schemas",
#                  prefix: nil,
#                  context: nil
#                }
#              } = EctoShorts.CommonSchema.get_loaded_struct(EctoShorts.Schema.Post)
#     end

#     test "returns struct with loaded state and source given {schema_source, schema_module}" do
#       assert %EctoShorts.Schema.AbstractPost{
#                __meta__: %Ecto.Schema.Metadata{
#                  state: :loaded,
#                  source: "concrete_table",
#                  prefix: nil,
#                  context: nil
#                }
#              } = EctoShorts.CommonSchema.get_loaded_struct({"concrete_table", AbstractSchema})
#     end

#     test "returns struct with loaded state, source, and prefix if @schema_prefix module attribute is set" do
#       assert %EctoShorts.Support.MockSchema.PrefixSchema{
#                __meta__: %Ecto.Schema.Metadata{
#                  state: :loaded,
#                  source: "prefix_schemas",
#                  prefix: "mock_schema_prefix",
#                  context: nil
#                }
#              } = EctoShorts.CommonSchema.get_loaded_struct(PrefixSchema)
#     end

#     test "returns struct with loaded state, source, and prefix given {schema_source, schema_module} if @schema_prefix module attribute is set" do
#       assert %EctoShorts.Support.MockSchema.PrefixSchema{
#                __meta__: %Ecto.Schema.Metadata{
#                  state: :loaded,
#                  source: "concrete_table",
#                  prefix: "mock_schema_prefix",
#                  context: nil
#                }
#              } = EctoShorts.CommonSchema.get_loaded_struct({"concrete_table", PrefixSchema})
#     end
#   end

#   describe "prefix_for_schema/2: " do
#     test "returns @schema_prefix module attribute value if set in schema" do
#       assert "mock_schema_prefix" = CommonSchema.prefix_for_schema(PrefixSchema)
#     end

#     test "returns nil if schema does not have @schema_prefix module attribute set" do
#       assert nil === CommonSchema.prefix_for_schema(AbstractSchema)
#     end

#     test "returns @schema_prefix module attribute value if set in schema and {schema_source, schema_module} tuple is given" do
#       assert "mock_schema_prefix" =
#                CommonSchema.prefix_for_schema({"concrete_table", PrefixSchema})
#     end

#     test "returns nil if schema does not have @schema_prefix module attribute set and {schema_source, schema_module} tuple is given" do
#       assert nil === CommonSchema.prefix_for_schema({"concrete_table", AbstractSchema})
#     end
#   end

#   describe "source_for_schema/2: " do
#     test "returns source defined in schema" do
#       assert "basic_schemas" = CommonSchema.source_for_schema(EctoShorts.Schema.Post)
#     end

#     test "returns source given {schema_source, schema_module}" do
#       assert "concrete_table" = CommonSchema.source_for_schema({"concrete_table", PrefixSchema})
#     end
#   end

#   describe "module_for_schema/2: " do
#     test "returns queryable module" do
#       assert EctoShorts.Schema.Post =
#                CommonSchema.module_for_schema(EctoShorts.Schema.Post)
#     end

#     test "returns queryable module given {schema_source, schema_module}" do
#       assert EctoShorts.Schema.AbstractPost =
#                CommonSchema.module_for_schema({"concrete_table", AbstractSchema})
#     end
#   end

#   describe "get_schema_query/1: " do
#     test "returns query struct" do
#       query = Ecto.Query.from(AbstractSchema)

#       assert ^query = CommonSchema.get_schema_query(query)
#     end

#     test "returns queryable" do
#       queryable = EctoShorts.CommonSchemaTest.MockSchema

#       assert ^queryable = CommonSchema.get_schema_query(queryable)
#     end

#     test "returns query where the from prefix is the value set by the @schema_prefix module attribute" do
#       query = CommonSchema.get_schema_query({"concrete_table", PrefixSchema})

#       assert %Ecto.Query{
#                from: %{
#                  prefix: "mock_schema_prefix",
#                  source: {"concrete_table", PrefixSchema}
#                }
#              } = query
#     end
#   end
# end
