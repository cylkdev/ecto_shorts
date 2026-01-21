defmodule EctoShorts.Actions.Transaction do
  alias EctoShorts.{
    Actions.Error,
    Config
  }

  @doc group: "CRUD"
  @doc """
  ...
  """
  def transaction(fun_or_multi, opts \\ []) do
    Config.repo!(opts).transaction(fun_or_multi, opts)
  end

  @doc group: "CRUD"
  @doc """
  ...
  """
  def transact(fun_or_multi, opts \\ [])

  def transact(%Ecto.Multi{} = multi, opts) do
    multi
    |> transaction(opts)
    |> handle_multi_response(opts)
  end

  def transact(fun, opts) when is_function(fun) do
    fn repo -> eval_transaction_fun(fun, repo, opts) end
    |> transaction(opts)
    |> normalize_transaction_response(opts)
  end

  defp normalize_transaction_response(result, opts) do
    unwrap? = Keyword.get(opts, :strict, true)

    case {unwrap?, result} do
      # If the transaction is rolled back with `:error`, Ecto
      # returns `{:error, :error}`. Normalize it to `:error`.
      {_, {:error, :error}} ->
        :error

      # If the transaction function returns `:ok`, Ecto returns
      # `{:ok, :ok}`. Normalize it to `:ok`.
      {_, {:ok, :ok}} ->
        :ok

      # If `unwrap?` is true, unwrap nested status tuples returned from
      # inside the transaction.
      #
      # `{:error, reason}` is commonly understood as “the transaction
      # failed / rolled back”, while `{:ok, {:error, reason}}` means
      # “the transaction committed and returned an error tuple as data”.
      #
      # For this reason, unwrapping is only done in strict mode. When
      # strict mode is enabled, returning `{:error, reason}` triggers
      # a rollback, so `{:ok, {:error, reason}}` should not happen in
      # normal usage.
      {true, {:ok, {:error, _} = error}} ->
        error

      # If `unwrap?` is true, unwrap nested ok tuples returned from
      # inside the transaction.
      #
      # This allows callers to return `{:ok, value}` from inside the
      # transaction without producing `{:ok, {:ok, value}}` after the
      # transaction commits.
      {true, {:ok, {:ok, _} = response}} ->
        response

      # Otherwise, return response unchanged.
      {_, other} ->
        other
    end
  end

  defp eval_transaction_fun(fun, repo, opts) do
    response = call_transaction_fun(fun, repo)

    if Keyword.get(opts, :strict, true) do
      maybe_rollback(response, repo)
    else
      response
    end
  end

  defp call_transaction_fun(fun, repo) when is_function(fun, 1), do: fun.(repo)
  defp call_transaction_fun(fun, _repo) when is_function(fun, 0), do: fun.()

  defp maybe_rollback(:error, repo), do: repo.rollback(:error)
  defp maybe_rollback({:error, reason}, repo), do: repo.rollback(reason)
  defp maybe_rollback({:ok, value}, _repo), do: value
  defp maybe_rollback(term, _repo), do: term

  defp handle_multi_response(
         {:error, _failed_operation, {code, message, details}, changes_so_far},
         opts
       ) do
    details =
      details
      |> Map.new()
      |> Map.put(:changes_so_far, Map.values(changes_so_far))

    {:error, Error.call(code, message, details, opts)}
  end

  defp handle_multi_response({:error, _failed_operation, reason, _changes_so_far}, _opts) do
    {:error, reason}
  end

  defp handle_multi_response({:ok, operations}, _opts) do
    {:ok, Map.values(operations)}
  end
end
