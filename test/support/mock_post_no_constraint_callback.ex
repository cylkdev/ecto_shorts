defmodule EctoShorts.Support.MockPostNoConstraintCallback do
  def changeset(model_or_changeset, params) do
    model_or_changeset
    |> EctoShorts.Support.Schemas.PostNoConstraint.changeset(params)
    |> Ecto.Changeset.no_assoc_constraint(:comments, name: "comments_post_id_fkey")
  end
end
