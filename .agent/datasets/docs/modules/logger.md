```elixir
defmodule Phoenix.Logger do
  @moduledoc """
  Instrumenter to handle logging of various instrumentation events.

  ## Instrumentation

  Phoenix uses the `:telemetry` library for instrumentation. The following events
  are published by Phoenix with the following measurements and metadata:

    * `[:phoenix, :endpoint, :init]` - dispatched by `Phoenix.Endpoint` after your
      Endpoint supervision tree successfully starts
      * Measurement: `%{system_time: system_time}`
      * Metadata: `%{pid: pid(), config: Keyword.t(), module: module(), otp_app: atom()}`
      * Disable logging: This event is not logged

    * `[:phoenix, :endpoint, :start]` - dispatched by `Plug.Telemetry` in your endpoint,
      usually after code reloading
      * Measurement: `%{system_time: system_time}`
      * Metadata: `%{conn: Plug.Conn.t, options: Keyword.t}`
      * Options: `%{log: Logger.level | false}`
      * Disable logging: In your endpoint `plug Plug.Telemetry, ..., log: Logger.level | false`
      * Configure log level dynamically: `plug Plug.Telemetry, ..., log: {Mod, Fun, Args}`

    * `[:phoenix, :endpoint, :stop]` - dispatched by `Plug.Telemetry` in your
      endpoint whenever the response is sent
      * Measurement: `%{duration: native_time}`
      * Metadata: `%{conn: Plug.Conn.t, options: Keyword.t}`
      * Options: `%{log: Logger.level | false}`
      * Disable logging: In your endpoint `plug Plug.Telemetry, ..., log: Logger.level | false`
      * Configure log level dynamically: `plug Plug.Telemetry, ..., log: {Mod, Fun, Args}`

    * `[:phoenix, :router_dispatch, :start]` - dispatched by `Phoenix.Router`
      before dispatching to a matched route
      * Measurement: `%{system_time: System.system_time}`
      * Metadata: `%{conn: Plug.Conn.t, route: binary, plug: module, plug_opts: term, path_params: map, pipe_through: [atom], log: Logger.level | false}`
      * Disable logging: Pass `log: false` to the router macro, for example: `get("/page", PageController, :index, log: false)`
      * Configure log level dynamically: `get("/page", PageController, :index, log: {Mod, Fun, Args})`

    * `[:phoenix, :router_dispatch, :exception]` - dispatched by `Phoenix.Router`
      after exceptions on dispatching a route
      * Measurement: `%{duration: native_time}`
      * Metadata: `%{conn: Plug.Conn.t, kind: :throw | :error | :exit, reason: term(), stacktrace: Exception.stacktrace()}`
      * Disable logging: This event is not logged

    * `[:phoenix, :router_dispatch, :stop]` - dispatched by `Phoenix.Router`
      after successfully dispatching a matched route
      * Measurement: `%{duration: native_time}`
      * Metadata: `%{conn: Plug.Conn.t, route: binary, plug: module, plug_opts: term, path_params: map, pipe_through: [atom], log: Logger.level | false}`
      * Disable logging: This event is not logged

    * `[:phoenix, :error_rendered]` - dispatched at the end of an error view being rendered
      * Measurement: `%{duration: native_time}`
      * Metadata: `%{conn: Plug.Conn.t, status: Plug.Conn.status, kind: Exception.kind, reason: term, stacktrace: Exception.stacktrace}`
      * Disable logging: Set `render_errors: [log: false]` on your endpoint configuration

    * `[:phoenix, :socket_connected]` - dispatched by `Phoenix.Socket`, at the end of a socket connection
      * Measurement: `%{duration: native_time}`
      * Metadata: `%{endpoint: atom, transport: atom, params: term, connect_info: map, vsn: binary, user_socket: atom, result: :ok | :error, serializer: atom, log: Logger.level | false}`
      * Disable logging: `use Phoenix.Socket, log: false` or `socket "/foo", MySocket, websocket: [log: false]` in your endpoint

    * `[:phoenix, :socket_drain]` - dispatched by `Phoenix.Socket` when using the `:drainer` option
      * Measurement: `%{count: integer, total: integer, index: integer, rounds: integer}`
      * Metadata: `%{endpoint: atom, socket: atom, intervasl: integer, log: Logger.level | false}`
      * Disable logging: `use Phoenix.Socket, log: false` in your endpoint or pass `:log` option in the `:drainer` option

    * `[:phoenix, :channel_joined]` - dispatched at the end of a channel join
      * Measurement: `%{duration: native_time}`
      * Metadata: `%{result: :ok | :error, params: term, socket: Phoenix.Socket.t}`
      * Disable logging: This event cannot be disabled

    * `[:phoenix, :channel_handled_in]` - dispatched at the end of a channel handle in
      * Measurement: `%{duration: native_time}`
      * Metadata: `%{event: binary, params: term, socket: Phoenix.Socket.t}`
      * Disable logging: This event cannot be disabled

  To see an example of how Phoenix LiveDashboard uses these events to create
  metrics, visit <https://hexdocs.pm/phoenix_live_dashboard/metrics.html>.

  ## Parameter filtering

  When logging parameters, Phoenix can filter out sensitive parameters
  such as passwords and tokens. Parameters to be filtered can be
  added via the `:filter_parameters` option:

      config :phoenix, :filter_parameters, ["password", "secret"]

  With the configuration above, Phoenix will filter any parameter
  that contains the terms `password` or `secret`. The match is
  case sensitive.

  Phoenix's default is `["password"]`.

  Phoenix can filter all parameters by default and selectively keep
  parameters. This can be configured like so:

      config :phoenix, :filter_parameters, {:keep, ["id", "order"]}

  With the configuration above, Phoenix will filter all parameters,
  except those that match exactly `id` or `order`. If a kept parameter
  matches, all parameters nested under that one will also be kept.

  ## Dynamic log level

  In some cases you may wish to set the log level dynamically
  on a per-request basis. To do so, set the `:log` option to
  a tuple, `{Mod, Fun, Args}`. The `Plug.Conn.t()` for the
  request will be prepended to the provided list of arguments.

  When invoked, your function must return a
  [`Logger.level()`](`t:Logger.level()/0`) or `false` to
  disable logging for the request.

  For example, in your Endpoint you might do something like this:

        # lib/my_app_web/endpoint.ex
        plug Plug.Telemetry,
          event_prefix: [:phoenix, :endpoint],
          log: {__MODULE__, :log_level, []}

        # Disables logging for routes like /status/*
        def log_level(%{path_info: ["status" | _]}), do: false
        def log_level(_), do: :info

  ## Disabling

  When you are using custom logging system it is not always desirable to enable
  `#{inspect(__MODULE__)}` by default. You can always disable this in general by:

      config :phoenix, :logger, false
  """

  require Logger

  @doc false
  def install  do
    # ...
  end

  @doc false
  def duration(duration)  do
    # ...
  end

  @doc false
  def compile_filter({:compiled, _key, _value} = filter)  do
    # ...
  end

  @doc false
  def filter_values(values, filter \\ Application.get_env(:phoenix, :filter_parameters, []))  do
    # ...
  end

  ## Event: [:phoenix, :endpoint, *]

  @doc false
  def phoenix_endpoint_start(_, _, %{conn: conn} = metadata, _)  do
    # ...
  end

  @doc false
  def phoenix_endpoint_stop(_, %{duration: duration}, %{conn: conn} = metadata, _)  do
    # ...
  end
end
```
