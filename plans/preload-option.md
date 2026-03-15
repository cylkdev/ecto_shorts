# Add `:preload` Option to All Struct-Returning Actions

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds. This document is the governing artifact for the task it describes. If the task later narrows to a specific boundary, test, or contract question, that later reasoning must still be recorded here unless the task is explicitly split into a separate ExecPlan.

See `.agent/PLANS.md` for the full ExecPlan standard that this document must be maintained in accordance with.

## Purpose / Big Picture

After this change, callers can pass `preload: [:comments]` (or any shape accepted by `Ecto.Repo.preload/3`) to every `EctoShorts.Actions` function that returns structs, and those associations will be loaded automatically before the result is returned. Today the caller must manually call `Actions.preload/3` as a second step; this change makes it a first-class option.

A naming conflict also existed: `insert_all/3` previously used `:preload` to mean something different (pre-fetching related data into the *params list* before insertion, via `batch_preload/4`). That option was renamed, and the function itself was renamed. See supersession note in Progress.

## In Scope

1. ~~Rename the `:preload` option in `insert_all/3` to `:batch_preload`~~ - **superseded**: the option is now `:batch_find` and the function is now `batch_find/4`. See `plans/batch-find-rename-and-preload-81133f.md`.
2. Add two private helpers in `lib/ecto_shorts/actions.ex`:
   - `maybe_preload(data, opts)` - calls `preload/3` when `opts[:preload]` is set and non-empty; no-ops on `nil` input.
   - `maybe_preload_ok(result, opts)` - unwraps `{:ok, value}`, preloads, re-wraps; passes `{:error, _}` through unchanged.
3. Wire `opts[:preload]` through every public function that returns structs, using the helpers above:
   - **CRUD**: `all/3`, `get/3`, `find/3`, `create/3`, `update/4`, `find_or_create/3`, `find_and_create/4`, `find_and_update/4`, `find_and_upsert/4`
   - **Multi**: `create_many/3`, `find_many/3`, `update_many/3`, `delete_many/3`, `find_or_create_many/3`, `find_and_upsert_many/3`
   - **Batch**: `batch/5` (walks map values)
4. Update `@doc` option tables for all changed functions to document `:preload`.
5. Add ExUnit tests covering `:preload` for representative CRUD, Multi, and Batch functions, plus a regression test verifying that `insert_all/3` with `:batch_preload` still works and `:preload` no longer triggers the old batch behavior.

## Out of Scope

- `update_all/4` and `delete_all/3` - return `{count, nil}`, no structs.
- `insert_all/3` `:preload` support for post-insert preloading - `insert_all` returns `{:ok, {count, nil}}` by default; `:returning` is an Ecto-native opt-in outside this task.
- `batch_preload/4` - this is a param-merging utility, not a struct-returning function.
- `delete/1,2,3` - returns the deleted struct; adding preload to deleted records is not useful.
- `stream/3` - returns an `Enumerable`, not a struct; preloading inside a lazy stream requires a transaction context the caller owns.
- `aggregate/5` - returns a scalar.

## Progress

- [x] (2026-03-15) ExecPlan written.
- [~] ~~Rename `:preload` → `:batch_preload` in `insert_all/3`~~ - **superseded** by `plans/batch-find-rename-and-preload-81133f.md`: `batch_preload/4` was renamed to `batch_find/4` and the option is `:batch_find` (2026-03-15).
- [x] Add `maybe_preload/2` and `maybe_preload_ok/2` private helpers.
- [x] Wire CRUD functions.
- [x] Wire Multi functions.
- [x] Wire `batch/5`.
- [x] Update docs.
- [ ] Write tests.
- [ ] Run full suite, verify green.

## Milestones

### Milestone 1 - Rename conflict and helpers

Rename `:preload` to `:batch_preload` in `insert_all/3`. Add the two private helpers. Compile and run the full suite to establish a green baseline before adding new behavior.

Acceptance: `mix compile --warnings-as-errors` passes; `mix test` passes (all existing tests green, including the insert_all tests that used `:preload`).

### Milestone 2 - Wire CRUD

Apply `maybe_preload` / `maybe_preload_ok` to all CRUD functions listed in scope. Update their `@doc` option tables.

Acceptance: Compile clean. Write at least two CRUD tests (`all/3` and `create/3`) that exercise `:preload`; run `mix test`.

### Milestone 3 - Wire Multi

Apply `maybe_preload_ok` at the call site of `run_multi/2` in each Multi function. Update docs.

Acceptance: Compile clean. Write one Multi test (`create_many/3`); run `mix test`.

### Milestone 4 - Wire Batch, Tests, Full Suite

Apply preload map-walk to `batch/5`. Update docs. Write the remaining tests (batch + regression for `insert_all` rename). Run `mix test` and confirm all pass.

## Surprises & Discoveries

None yet.

## Decision Log

- Decision: Implement preload at the `actions.ex` call site for Multi functions rather than inside `run_multi/2`.
  Rationale: `run_multi/2` does not carry schema context and the opts are visible at the public function level; piping at the call site keeps the transformation local to each function boundary and avoids leaking preload logic into the shared helper.
  Date/Author: 2026-03-15

- Decision: `batch/5` preload walks map values with `Enum.map` and rebuilds the map.
  Rationale: `batch` returns `%{key => struct | [struct]}`. After the existing result map is built, walking values and preloading each entry is the only correct approach; we cannot preload before grouping.
  Date/Author: 2026-03-15

- Decision: `maybe_preload(nil, opts)` returns `nil` unchanged (for `get/3`).
  Rationale: `get/3` returns `nil` when a record is not found; calling `repo.preload(nil, ...)` would raise.
  Date/Author: 2026-03-15

## Outcomes & Retrospective

Not yet complete.

## Context and Orientation

The file under edit is `lib/ecto_shorts/actions.ex` (approximately 1991 lines). All public CRUD, Multi, and Batch operations live there. Internal delegate modules (`actions/bulk.ex`, `actions/multi.ex`, `actions/batch.ex`, `actions/transaction.ex`, `actions/error.ex`) are not changed.

The existing `preload/3` public function in `actions.ex` (line ~766) calls `Config.replica!(opts).preload(data, preloads, opts)`. The new helpers will call this same function.

Test schemas: `EctoShorts.Schema.Post` has `has_many :comments, EctoShorts.Schema.Comment` and `belongs_to :author, EctoShorts.Schema.User`. These associations are available in tests.

Test files live in `test/ecto_shorts/actions/`. Patterns use `EctoShorts.DataCase` with `async: true`, direct `Repo` calls for setup, and pattern-matching assertions.

## Plan of Work

**Step 1 - Rename in `insert_all/3`** *(superseded)*

Originally planned to rename `:preload` → `:batch_preload`. The function and option were subsequently renamed to `batch_find/4` and `:batch_find` respectively by `plans/batch-find-rename-and-preload-81133f.md`. Live code now uses `:batch_find`.

**Step 2 - Private helpers**

Add near the bottom of `actions.ex`, before the existing `put_param/3`:

    defp maybe_preload(nil, _opts), do: nil
    defp maybe_preload(data, opts) do
      case opts[:preload] do
        nil -> data
        [] -> data
        preloads -> preload(data, preloads, opts)
      end
    end

    defp maybe_preload_ok({:ok, value}, opts), do: {:ok, maybe_preload(value, opts)}
    defp maybe_preload_ok(other, _opts), do: other

**Step 3 - CRUD wiring**

For each function, pipe the final result through the appropriate helper:

- `all/3`: add `|> maybe_preload(opts)` after `Config.repo!(opts).all(opts)`.
- `get/3`: add `|> maybe_preload(opts)` after `Config.replica!(opts).get(queryable, id, opts)`.
- `find/3`: in the success branch `{:ok, record}`, return `{:ok, maybe_preload(record, opts)}`.
- `create/3`: pipe repo insert result through `maybe_preload_ok(opts)`.
- `update/4` (struct branch): pipe repo update result through `maybe_preload_ok(opts)`.
- `find_or_create/3`, `find_and_create/4`, `find_and_update/4`, `find_and_upsert/4`: pipe their final `{:ok, struct}` result through `maybe_preload_ok(opts)`.

**Step 4 - Multi wiring**

In each `*_many` public function, pipe `run_multi(...)` through `maybe_preload_ok(opts)`.

**Step 5 - Batch wiring**

In `batch/5`, after the result map is computed, apply:

    result_map
    |> Map.new(fn {k, v} -> {k, maybe_preload(v, opts)} end)

This works for both `:one` (value is a struct) and `:many` (value is a list of structs), since `Ecto.Repo.preload/3` accepts both.

**Step 6 - Docs**

For each modified function, add to the options list:

    * `:preload` - associations to preload on the result. Accepts the same shapes
      as `preload/3`: an atom, list of atoms, keyword list for nested preloads,
      or `{assoc, query}` tuple. Applied after the operation completes.

For `insert_all/3`, rename `:preload` to `:batch_preload` in the option description.

## Internal Boundary Contracts

**`maybe_preload/2`**

- Upstream callers: all public functions returning bare structs or lists.
- Input: `data` is `nil | struct() | [struct()]`, `opts` is `keyword()`.
- Output: same type as input - `nil | struct() | [struct()]`.
- Invariant: if `opts[:preload]` is nil or `[]`, returns `data` unchanged.
- Does NOT accept: `{:ok, _}` tuples - those go through `maybe_preload_ok/2`.

**`maybe_preload_ok/2`**

- Upstream callers: all public functions returning `{:ok, struct()} | {:ok, [struct()]} | {:error, _}`.
- Input: `{:ok, value}` or `{:error, reason}` with `opts`.
- Output: `{:ok, preloaded_value}` or `{:error, reason}` unchanged.
- Delegates to `maybe_preload/2` for the inner value.

## Example Mappings

### Story: `:preload` on `all/3`

A caller fetches posts and wants comments loaded in the same call.

#### Rules:

- When `:preload` is set to a non-empty list, associations are loaded after the query.
- When `:preload` is `nil` or absent, the result is returned as-is (no extra query).
- When `:preload` is `[]`, treated as absent (no preload).

#### Examples:

    Actions.all(Post, %{id: post.id}, preload: [:comments])
    # => [%Post{comments: [%Comment{...}]}]

    Actions.all(Post, %{id: post.id})
    # => [%Post{comments: #Ecto.Association.NotLoaded<...>}]

#### Open Questions:

- **Q:** Should passing `preload: []` be a no-op? **A:** Yes, treated as absent.

## Behaviour Specifications

### Feature: `:preload` on struct-returning actions

Scenario: Caller passes `preload: [:comments]` to `all/3`
  Given a Post with one associated Comment exists in the database
  When the caller calls `Actions.all(Post, %{id: post.id}, preload: [:comments])`
  Then the result is `[%Post{comments: [%Comment{...}]}]` with comments loaded

Scenario: Caller omits `:preload` from `all/3`
  Given a Post exists
  When the caller calls `Actions.all(Post, %{id: post.id})`
  Then `comments` is `%Ecto.Association.NotLoaded{}`

Scenario: Caller passes `preload: [:comments]` to `create/3`
  Given the schema and repo are configured
  When the caller calls `Actions.create(Post, %{title: "Hi"}, preload: [:comments])`
  Then the result is `{:ok, %Post{comments: []}}` with comments loaded (empty list)

Scenario: `:batch_preload` rename in `insert_all/3`
  Given params with a `:batch_preload` key
  When `insert_all/3` is called with `batch_preload: :id`
  Then the batch preload behavior runs as before

## Executable Tests

Tests live in the existing files under `test/ecto_shorts/actions/`.

CRUD tests go in `actions_crud_test.exs`:

    describe "all/3 with :preload" do
      test "preloads associations on returned structs" do
        post = Repo.insert!(%Post{} |> Post.changeset(%{title: "T"}))
        Repo.insert!(%Comment{} |> Comment.changeset(%{body: "Hi", post_id: post.id}))
        [result] = Actions.all(Post, %{id: post.id}, preload: [:comments])
        assert [%Comment{}] = result.comments
      end
    end

    describe "create/3 with :preload" do
      test "preloads associations on the created struct" do
        {:ok, post} = Actions.create(Post, %{title: "T"}, preload: [:comments])
        assert post.comments == []
      end
    end

Multi tests go in `actions_multi_test.exs`:

    describe "create_many/3 with :preload" do
      test "preloads associations on all created structs" do
        {:ok, posts} = Actions.create_many(Post, [%{title: "A"}, %{title: "B"}], preload: [:comments])
        assert Enum.all?(posts, fn p -> p.comments == [] end)
      end
    end

Batch tests go in `actions_batch_test.exs`:

    describe "batch/5 with :preload" do
      test "preloads associations on batched structs" do
        post = Repo.insert!(%Post{} |> Post.changeset(%{title: "X"}))
        result = Actions.batch(Post, [%{title: "X"}], :title, :one, preload: [:comments])
        assert %Post{comments: []} = result["X"]
      end
    end

Bulk regression test goes in `actions_bulk_test.exs`:

    test "batch_preload option still works after rename" do
      # insert_all with :batch_preload should not raise
      assert {:ok, _} = Actions.insert_all(Post, [%{title: "R"}], [])
    end

## Validation and Acceptance

Run from the repo root:

    mix compile --warnings-as-errors
    mix test

Expected: all existing tests pass plus the new preload tests pass. Zero warnings.

## Idempotence and Recovery

All changes are additive or minimal renames. Running `mix test` multiple times is safe. If tests fail, check whether `maybe_preload` is being called with a `{:ok, _}` tuple (wrong helper) or whether `batch/5` is walking a map that still contains raw results before grouping.
