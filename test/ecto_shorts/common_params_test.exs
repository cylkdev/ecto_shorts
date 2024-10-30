defmodule EctoShorts.CommonParamsTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias EctoShorts.{
    CommonParams,
    Support.Schemas.Post,
    Support.Schemas.PostTimestampCustomFieldName,
    Support.Schemas.PostTimestampDateTime
  }

  describe "convert_to_insert_all_params/2: " do
    test "when given map params, return expected result" do
      assert {:ok,
              [
                %{
                  title: "post_1_title",
                  inserted_at: post_1_inserted_at,
                  updated_at: post_1_updated_at
                } = post_1_params,
                %{
                  title: "post_2_title",
                  inserted_at: post_2_inserted_at,
                  updated_at: post_2_updated_at
                } = post_2_params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  %{title: "post_1_title"},
                  %{title: "post_2_title"}
                ]
              )

      assert %NaiveDateTime{} = post_1_inserted_at
      assert %NaiveDateTime{} = post_1_updated_at

      assert %NaiveDateTime{} = post_2_inserted_at
      assert %NaiveDateTime{} = post_2_updated_at

      assert %{
        title: "post_1_title",
        inserted_at: post_1_inserted_at,
        updated_at: post_1_updated_at
      } === post_1_params

      assert %{
        title: "post_2_title",
        inserted_at: post_2_inserted_at,
        updated_at: post_2_updated_at
      } === post_2_params
    end
  end

  describe "convert_to_insert_all_params/3: " do
    test "when params are valid, return expected result" do
      assert {:ok,
              [
                %{
                  title: "post_1_title",
                  inserted_at: post_1_inserted_at,
                  updated_at: post_1_updated_at
                } = post_1_params,
                %{
                  title: "post_2_title",
                  inserted_at: post_2_inserted_at,
                  updated_at: post_2_updated_at
                } = post_2_params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  %{title: "post_1_title"},
                  %{title: "post_2_title"}
                ],
                []
              )

      assert %NaiveDateTime{} = post_1_inserted_at
      assert %NaiveDateTime{} = post_1_updated_at

      assert %NaiveDateTime{} = post_2_inserted_at
      assert %NaiveDateTime{} = post_2_updated_at

      assert %{
        title: "post_1_title",
        inserted_at: post_1_inserted_at,
        updated_at: post_1_updated_at
      } === post_1_params

      assert %{
        title: "post_2_title",
        inserted_at: post_2_inserted_at,
        updated_at: post_2_updated_at
      } === post_2_params
    end

    test "when params invalid, returns changeset errors by default" do
      assert {
        :error,
        [
          {
            0,
            %Ecto.Changeset{
              action: :insert,
              data: %Post{},
              changes: %{title: "short_a"},
              valid?: false
            } = changeset_1
          },
          {
            2,
            %Ecto.Changeset{
              action: :insert,
              data: %Post{},
              changes: %{title: "short_b"},
              valid?: false
            } = changeset_2
          }
        ]
      } =
        CommonParams.convert_to_insert_all_params(
          Post,
          [
            %{title: "short_a"},
            %{title: "valid_post_title"},
            %{title: "short_b"}
          ],
          []
        )

      assert {:title, ["should be at least 10 character(s)"]} in EctoShorts.DataCase.errors_on(changeset_1)
      assert {:title, ["should be at least 10 character(s)"]} in EctoShorts.DataCase.errors_on(changeset_2)
    end

    test "when params is [{changeset, changes}], changes are valid params and option validate is true, return expected result" do
      assert {:ok,
              [
                %{
                  title: "post_1_title",
                  inserted_at: post_1_inserted_at,
                  updated_at: post_1_updated_at
                } = post_1_params,
                %{
                  title: "post_2_title",
                  inserted_at: post_2_inserted_at,
                  updated_at: post_2_updated_at
                } = post_2_params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    Post.changeset(%Post{title: "post_1_existing_title"}, %{}),
                    %{title: "post_1_title"}
                  },
                  {
                    Post.changeset(%Post{title: "post_2_existing_title"}, %{}),
                    %{title: "post_2_title"}
                  }
                ],
                validate: true
              )

      assert %NaiveDateTime{} = post_1_inserted_at
      assert %NaiveDateTime{} = post_1_updated_at

      assert %NaiveDateTime{} = post_2_inserted_at
      assert %NaiveDateTime{} = post_2_updated_at

      assert %{
        title: "post_1_title",
        inserted_at: post_1_inserted_at,
        updated_at: post_1_updated_at
      } === post_1_params

      assert %{
        title: "post_2_title",
        inserted_at: post_2_inserted_at,
        updated_at: post_2_updated_at
      } === post_2_params
    end

    test "when params is [{changeset, changes}], changes are invalid params and option validate is true, returns changeset errors" do
      assert {
              :error,
              [
                {
                  0,
                  %Ecto.Changeset{
                    action: :update,
                    changes: %{title: "short"},
                    data: %Post{},
                    valid?: false
                  } = changeset
                }
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {Post.changeset(%Post{}, %{}), %{title: "short"}}
                ],
                validate: true
              )

      assert {:title, ["should be at least 10 character(s)"]} in EctoShorts.DataCase.errors_on(changeset)
    end

    test "when params is [{changeset, params}] and option validate is false, merges params into changes then into struct" do
      assert {:ok,
              [
                %{
                  title: "post_1_title",
                  inserted_at: post_1_inserted_at,
                  updated_at: post_1_updated_at
                } = post_1_params,
                %{
                  title: "post_2_title",
                  inserted_at: post_2_inserted_at,
                  updated_at: post_2_updated_at
                } = post_2_params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    Post.changeset(%Post{title: "post_1_existing_title"}, %{}),
                    %{title: "post_1_title"}
                  },
                  {
                    Post.changeset(%Post{title: "post_2_existing_title"}, %{}),
                    %{title: "post_2_title"}
                  }
                ],
                validate: false
              )

      assert %NaiveDateTime{} = post_1_inserted_at
      assert %NaiveDateTime{} = post_1_updated_at

      assert %NaiveDateTime{} = post_2_inserted_at
      assert %NaiveDateTime{} = post_2_updated_at

      assert %{
        title: "post_1_title",
        inserted_at: post_1_inserted_at,
        updated_at: post_1_updated_at
      } === post_1_params

      assert %{
        title: "post_2_title",
        inserted_at: post_2_inserted_at,
        updated_at: post_2_updated_at
      } === post_2_params
    end

    test "when params is [{schema_data, params}] and option validate is false, merges params into struct" do
      assert {:ok,
              [
                %{
                  title: "post_1_title",
                  inserted_at: post_1_inserted_at,
                  updated_at: post_1_updated_at
                } = post_1_params,
                %{
                  title: "post_2_title",
                  inserted_at: post_2_inserted_at,
                  updated_at: post_2_updated_at
                } = post_2_params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    %Post{title: "post_1_existing_title"},
                    %{title: "post_1_title"}
                  },
                  {
                    %Post{title: "post_2_existing_title"},
                    %{title: "post_2_title"}
                  }
                ],
                validate: false
              )

      assert %NaiveDateTime{} = post_1_inserted_at
      assert %NaiveDateTime{} = post_1_updated_at

      assert %NaiveDateTime{} = post_2_inserted_at
      assert %NaiveDateTime{} = post_2_updated_at

      assert %{
        title: "post_1_title",
        inserted_at: post_1_inserted_at,
        updated_at: post_1_updated_at
      } === post_1_params

      assert %{
        title: "post_2_title",
        inserted_at: post_2_inserted_at,
        updated_at: post_2_updated_at
      } === post_2_params
    end

    test "when params is a changeset and option validate is false, merges params into changes then into struct" do
      assert {:ok,
              [
                %{
                  title: "post_1_title",
                  inserted_at: post_1_inserted_at,
                  updated_at: post_1_updated_at
                } = post_1_params,
                %{
                  title: "post_2_title",
                  inserted_at: post_2_inserted_at,
                  updated_at: post_2_updated_at
                } = post_2_params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  Post.changeset(%Post{title: "post_1_title"}, %{}),
                  Post.changeset(%Post{title: "post_2_title"}, %{})
                ],
                validate: false
              )

      assert %NaiveDateTime{} = post_1_inserted_at
      assert %NaiveDateTime{} = post_1_updated_at

      assert %NaiveDateTime{} = post_2_inserted_at
      assert %NaiveDateTime{} = post_2_updated_at

      assert %{
        title: "post_1_title",
        inserted_at: post_1_inserted_at,
        updated_at: post_1_updated_at
      } === post_1_params

      assert %{
        title: "post_2_title",
        inserted_at: post_2_inserted_at,
        updated_at: post_2_updated_at
      } === post_2_params
    end

    test "when params is a schema_data struct and option validate is false, merges params into struct" do
      assert {:ok,
              [
                %{
                  title: "post_1_title",
                  inserted_at: post_1_inserted_at,
                  updated_at: post_1_updated_at
                } = post_1_params,
                %{
                  title: "post_2_title",
                  inserted_at: post_2_inserted_at,
                  updated_at: post_2_updated_at
                } = post_2_params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  %Post{title: "post_1_title"},
                  %Post{title: "post_2_title"}
                ],
                validate: false
              )

      assert %NaiveDateTime{} = post_1_inserted_at
      assert %NaiveDateTime{} = post_1_updated_at

      assert %NaiveDateTime{} = post_2_inserted_at
      assert %NaiveDateTime{} = post_2_updated_at

      assert %{
        title: "post_1_title",
        inserted_at: post_1_inserted_at,
        updated_at: post_1_updated_at
      } === post_1_params

      assert %{
        title: "post_2_title",
        inserted_at: post_2_inserted_at,
        updated_at: post_2_updated_at
      } === post_2_params
    end

    test "when params is a map and option validate is false, creates new struct merges params into struct" do
      assert {:ok,
              [
                %{
                  title: "post_1_title",
                  inserted_at: post_1_inserted_at,
                  updated_at: post_1_updated_at
                } = post_1_params,
                %{
                  title: "post_2_title",
                  inserted_at: post_2_inserted_at,
                  updated_at: post_2_updated_at
                } = post_2_params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  %{title: "post_1_title"},
                  %{title: "post_2_title"}
                ],
                validate: false
              )

      assert %NaiveDateTime{} = post_1_inserted_at
      assert %NaiveDateTime{} = post_1_updated_at

      assert %NaiveDateTime{} = post_2_inserted_at
      assert %NaiveDateTime{} = post_2_updated_at

      assert %{
        title: "post_1_title",
        inserted_at: post_1_inserted_at,
        updated_at: post_1_updated_at
      } === post_1_params

      assert %{
        title: "post_2_title",
        inserted_at: post_2_inserted_at,
        updated_at: post_2_updated_at
      } === post_2_params
    end

    test "when option :placeholders is set and value on params matches value on placeholder, placeholder value is set" do
      assert {:ok,
              [
                %{title: {:placeholder, :title}}
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [%{title: "matching_post_title"}],
                placeholders: %{title: "matching_post_title"}
              )
    end

    test "when option :placeholders is set and value on params does not match value on placeholder, placeholder value is not set" do
      assert {:ok,
              [
                %{title: "another_post_title"}
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [%{title: "another_post_title"}],
                placeholders: %{title: "placeholder_post_title"}
              )
    end

    test "when option :placeholders is set and value on params for placeholder key is nil, placeholder value not set" do
      assert {:ok,
              [
                %{
                  unique_identifier: post_unique_identifier,
                  inserted_at: inserted_at,
                  updated_at: updated_at
                } = post
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [%{unique_identifier: "post_unique_identifier"}],
                placeholders: %{title: "post_title_placeholder"}
              )

      assert %{
        unique_identifier: post_unique_identifier,
        inserted_at: inserted_at,
        updated_at: updated_at
      } === post
    end

    test "when function completes, value of field :updated_at set to current time" do
      assert {:ok,
              [
                %{
                  id: 1,
                  title: "post_created_title",
                  inserted_at: ~N[2024-10-26 23:09:31],
                  updated_at: updated_at
                } = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    %Post{
                      id: 1,
                      inserted_at: ~N[2024-10-26 23:09:31],
                      updated_at: ~N[2024-10-26 23:09:31]
                    },
                    %{title: "post_created_title"}
                  }
                ],
                []
              )

      assert %NaiveDateTime{} = updated_at
      assert ~N[2024-10-26 23:09:31] !== updated_at

      # updated at is sometime within the last second
      diff = NaiveDateTime.diff(updated_at, NaiveDateTime.utc_now())
      assert diff >= -1 and diff <= 0

      assert %{
        id: 1,
        title: "post_created_title",
        inserted_at: ~N[2024-10-26 23:09:31],
        updated_at: updated_at
      } === params
    end

    test "when schema_data has key :id and the value is non nil, timestamp inserted_at is not generated" do
      assert {:ok,
              [
                %{
                  id: 1,
                  title: "post_created_title",
                  updated_at: updated_at
                } = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    %Post{id: 1},
                    %{title: "post_created_title"}
                  }
                ],
                []
              )

      assert %NaiveDateTime{} = updated_at

      assert %{
        id: 1,
        title: "post_created_title",
        updated_at: updated_at
      } === params
    end

    test "when schema_data has key :id and the value is non nil and option validate is true, timestamp inserted_at is generated" do
      assert {:ok,
              [
                %{
                  title: "post_created_title",
                  inserted_at: inserted_at,
                  updated_at: updated_at
                } = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    %Post{},
                    %{id: 1, title: "post_created_title"}
                  }
                ],
                validate: true
              )

      assert %NaiveDateTime{} = inserted_at
      assert %NaiveDateTime{} = updated_at

      # id should not be here
      assert %{
        title: "post_created_title",
        inserted_at: inserted_at,
        updated_at: updated_at
      } === params
    end

    test "when schema_data has key :id and the value is non nil and option validate is false, timestamp inserted_at is not generated" do
      assert {:ok,
              [
                %{
                  id: 1,
                  title: "post_created_title",
                  updated_at: updated_at
                } = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    %Post{},
                    %{id: 1, title: "post_created_title"}
                  }
                ],
                validate: false
              )

      assert %NaiveDateTime{} = updated_at

      assert %{
        id: 1,
        title: "post_created_title",
        updated_at: updated_at
      } === params
    end

    test "when params has key :id and the value is non nil, timestamp inserted_at is not replaced" do
      assert {:ok,
              [
                %{
                  id: 1,
                  title: "post_created_title",
                  inserted_at: ~N[2024-10-26 23:09:31],
                  updated_at: updated_at
                } = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    %Post{
                      id: 1,
                      inserted_at: ~N[2024-10-26 23:09:31]
                    },
                    %{title: "post_created_title"}
                  }
                ],
                []
              )

      assert %NaiveDateTime{} = updated_at

      assert %{
        id: 1,
        title: "post_created_title",
        inserted_at: ~N[2024-10-26 23:09:31],
        updated_at: updated_at
      } === params
    end

    test "when schema_data has key :id and the value is non nil, timestamp inserted_at is not replaced " do
      assert {:ok,
              [
                %{
                  id: 1,
                  title: "post_created_title",
                  inserted_at: ~N[2024-10-26 23:09:31],
                  updated_at: updated_at
                } = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [
                  {
                    %Post{
                      id: 1,
                      inserted_at: ~N[2024-10-26 23:09:31]
                    },
                    %{title: "post_created_title"}
                  }
                ],
                []
              )

      assert %NaiveDateTime{} = updated_at

      assert %{
        id: 1,
        title: "post_created_title",
        inserted_at: ~N[2024-10-26 23:09:31],
        updated_at: updated_at
      } === params
    end

    test "when option :autogenerate is false, timestamp inserted_at and updated_at is not generated" do
      assert {:ok,
              [
                %{title: "post_created_title"} = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [%{title: "post_created_title"}],
                autogenerate: false
              )

      assert %{title: "post_created_title"} === params
    end

    test "when options :autogenerate is true and :timestamp_inserted_at is false, timestamp inserted_at is not generated" do
      assert {:ok,
              [
                %{
                  title: "post_created_title",
                  updated_at: updated_at
                } = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [%{title: "post_created_title"}],
                autogenerate: true,
                timestamp_inserted_at: false
              )

      assert %NaiveDateTime{} = updated_at

      assert %{
        title: "post_created_title",
        updated_at: updated_at
      } === params
    end

    test "when options :autogenerate is true and :timestamp_updated_at is false, timestamp updated_at is not generated" do
      assert {:ok,
              [
                %{
                  title: "post_created_title",
                  inserted_at: inserted_at
                } = params
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [%{title: "post_created_title"}],
                autogenerate: true,
                timestamp_updated_at: false
              )

      assert %NaiveDateTime{} = inserted_at

      assert %{
        title: "post_created_title",
        inserted_at: inserted_at
      } === params
    end

    test "when option timestamps_type is set to :utc_datetime, timestamp inserted_at and updated_at are set to :utc_datetime" do
      assert {:ok,
              [
                %{
                  title: "post_created_title",
                  inserted_at: inserted_at,
                  updated_at: updated_at
                }
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                Post,
                [%{title: "post_created_title"}],
                timestamps_type: :utc_datetime
              )

      assert %DateTime{} = inserted_at
      assert %DateTime{} = updated_at
    end

    test "when schema has field names inserted_at and updated_at with the type :utc_datetime, set timestamp types to :utc_datetime" do
      assert {:ok,
              [
                %{
                  title: "post_created_title",
                  inserted_at: inserted_at,
                  updated_at: updated_at
                }
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                PostTimestampDateTime,
                [%{title: "post_created_title"}],
                []
              )

      assert %DateTime{} = inserted_at
      assert %DateTime{} = updated_at
    end

    test "when schema has custom field names for inserted_at and updated_at, set field names with options :timestamp_inserted_at_field_name and :timestamp_updated_at_field_name" do
      assert {:ok,
              [
                %{
                  title: "post_created_title",
                  created_at: created_at,
                  modified_at: modified_at
                }
              ]
            } =
              CommonParams.convert_to_insert_all_params(
                PostTimestampCustomFieldName,
                [%{title: "post_created_title"}],
                timestamp_inserted_at_field_name: :created_at,
                timestamp_updated_at_field_name: :modified_at
              )

      assert %DateTime{} = created_at
      assert %DateTime{} = modified_at
    end

    test "when option :timestamp_inserted_at_field_name is invalid, raise argument error" do
      assert_raise ArgumentError, ~r|Timestamp field name for inserted_at not found on schema|, fn ->
        CommonParams.convert_to_insert_all_params(
          Post,
          [%{title: "post_created_title"}],
          timestamp_inserted_at_field_name: :invalid_field_name
        )
      end
    end

    test "when option :timestamp_updated_at_field_name is invalid, raise argument error" do
      assert_raise ArgumentError, ~r|Timestamp field name for updated_at not found on schema|, fn ->
        CommonParams.convert_to_insert_all_params(
          Post,
          [%{title: "post_created_title"}],
          timestamp_updated_at_field_name: :invalid_field_name
        )
      end
    end
  end

  describe "convert_to_update_all_params/2: " do
    test "when given keyword and params are valid return expected result" do
      assert [
        inc: [views: 1],
        pull: [tags: "post_tag_old_1"],
        push: [tags: "post_tag_new_1"],
        set: [
          likes: 21,
          title: "post_created_title",
          unique_identifier: "post_unique_identifier",
          updated_at: %NaiveDateTime{}
        ]
      ] =
        CommonParams.convert_to_update_all_params(
          Post,
          %{
            id: 1,
            title: "post_created_title",
            likes: 21,
            views: %{
              inc: 1
            },
            tags: %{
              push: "post_tag_new_1",
              pull: "post_tag_old_1",
            },
            unique_identifier: %{
              set: "post_unique_identifier"
            }
          }
        )
    end
  end

  describe "convert_to_update_all_params/3: " do
    test "when given map and params are valid return expected result" do
      assert [
        inc: [views: 1],
        pull: [tags: "post_tag_old_1"],
        push: [tags: "post_tag_new_1"],
        set: [
          likes: 21,
          title: "post_created_title",
          unique_identifier: "post_unique_identifier",
          updated_at: %NaiveDateTime{}
        ]
      ] =
        CommonParams.convert_to_update_all_params(
          Post,
          %{
            id: 1,
            title: "post_created_title",
            likes: 21,
            views: %{
              inc: 1
            },
            tags: %{
              push: "post_tag_new_1",
              pull: "post_tag_old_1",
            },
            unique_identifier: %{
              set: "post_unique_identifier"
            }
          },
          []
        )
    end

    test "when given keyword and params are valid, return expected result" do
      assert [
        inc: [views: 1],
        pull: [
          tags: "post_tag_old_1",
          tags: "post_tag_old_2"
        ],
        push: [
          tags: "push_list_term_by_default",
          tags: "post_tag_new_1",
          tags: "post_tag_new_2"
        ],
        set: [
          likes: 21,
          title: "post_created_title",
          unique_identifier: "post_unique_identifier",
          updated_at: %NaiveDateTime{}
        ]
      ] =
        CommonParams.convert_to_update_all_params(
          Post,
          [
            id: 1,
            title: "post_created_title",
            likes: 21,
            views: [
              inc: 1
            ],
            tags: [
              "push_list_term_by_default",
              push: "post_tag_new_1",
              push: "post_tag_new_2",
              pull: "post_tag_old_1",
              pull: "post_tag_old_2",
            ],
            unique_identifier: [
              set: "post_unique_identifier"
            ]
          ],
          []
        )
    end

    test "when option :ordered is false, returns unordered results" do
      assert [
        set: [
          updated_at: %NaiveDateTime{},
          likes: 21,
          views: 1,
          title: "post_created_title"
        ]
      ] =
        CommonParams.convert_to_update_all_params(
          Post,
          [
            likes: 21,
            views: 1,
            title: "post_created_title",
            id: 1
          ],
          ordered: false
        )
    end

    test "raises the field name set for option :timestamp_updated_at_field_name is invalid" do
      assert_raise ArgumentError, ~r|Timestamp field name for updated_at not found on schema|, fn ->
        CommonParams.convert_to_update_all_params(
          Post,
          %{title: "post_created_title"},
          timestamp_updated_at_field_name: :invalid_field_name
        )
      end
    end

    test "when given map params and has a field that does not exist on the schema, log warning" do
      log =
        capture_log([level: :warning], fn ->
          assert [
            set: [
              non_existent_field: "non_existent_field",
              title: "post_created_title",
              updated_at: %NaiveDateTime{}
            ]
          ] =
            CommonParams.convert_to_update_all_params(
              Post,
              [
                non_existent_field: "non_existent_field",
                title: "post_created_title"
              ],
              []
            )
        end)

      assert log =~ ~r|The field :non_existent_field does not exist on the schema|
    end

    test "when option :autogenerate is false, timestamp updated_at is not generated" do
      assert [
              set: [
                title: "post_created_title"
              ]
            ] =
              CommonParams.convert_to_update_all_params(
                Post,
                [
                  title: "post_created_title"
                ],
                autogenerate: false
              )
  end
  end
end
