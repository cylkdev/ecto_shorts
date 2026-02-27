```elixir
defmodule Phoenix.Socket do
  @moduledoc ~S"""
  A socket implementation that multiplexes messages over channels.

  `Phoenix.Socket` is used as a module for establishing a connection
  between client and server. Once the connection is established,
  the initial state is stored in the `Phoenix.Socket` struct.

  The same socket can be used to receive events from different transports.
  Phoenix supports `websocket` and `longpoll` options when invoking
  `Phoenix.Endpoint.socket/3` in your endpoint. `websocket` is set by default
  and `longpoll` can also be configured explicitly.

      socket "/socket", MyAppWeb.Socket, websocket: true, longpoll: false

  The command above means incoming socket connections can be made via
  a WebSocket connection. Incoming and outgoing events are routed to
  channels by topic:

      channel "room:lobby", MyAppWeb.LobbyChannel

  See `Phoenix.Channel` for more information on channels.

  ## Socket Behaviour

  Socket handlers are mounted in Endpoints and must define two callbacks:

    * `connect/3` - receives the socket params, connection info if any, and
      authenticates the connection. Must return a `Phoenix.Socket` struct,
      often with custom assigns

    * `id/1` - receives the socket returned by `connect/3` and returns the
      id of this connection as a string. The `id` is used to identify socket
      connections, often to a particular user, allowing us to force disconnections.
      For sockets requiring no authentication, `nil` can be returned

  ## Examples

      defmodule MyAppWeb.UserSocket do
        use Phoenix.Socket

        channel "room:*", MyAppWeb.RoomChannel

        def connect(params, socket, _connect_info) do
          {:ok, assign(socket, :user_id, params["user_id"])}
        end

        def id(socket), do: "users_socket:#{socket.assigns.user_id}"
      end

      # Disconnect all user's socket connections and their multiplexed channels
      MyAppWeb.Endpoint.broadcast("users_socket:" <> user.id, "disconnect", %{})

  ## Socket fields

    * `:id` - The string id of the socket
    * `:assigns` - The map of socket assigns, default: `%{}`
    * `:channel` - The current channel module
    * `:channel_pid` - The channel pid
    * `:endpoint` - The endpoint module where this socket originated, for example: `MyAppWeb.Endpoint`
    * `:handler` - The socket module where this socket originated, for example: `MyAppWeb.UserSocket`
    * `:joined` - If the socket has effectively joined the channel
    * `:join_ref` - The ref sent by the client when joining
    * `:ref` - The latest ref sent by the client
    * `:pubsub_server` - The registered name of the socket's pubsub server
    * `:topic` - The string topic, for example `"room:123"`
    * `:transport` - An identifier for the transport, used for logging
    * `:transport_pid` - The pid of the socket's transport process
    * `:serializer` - The serializer for socket messages

  ## Using options

  On `use Phoenix.Socket`, the following options are accepted:

    * `:log` - the default level to log socket actions. Defaults
      to `:info`. May be set to `false` to disable it

    * `:partitions` - each channel is spawned under a supervisor.
      This option controls how many supervisors will be spawned
      to handle channels. Defaults to the number of cores.

  ## Garbage collection

  It's possible to force garbage collection in the transport process after
  processing large messages. For example, to trigger such from your channels,
  run:

      send(socket.transport_pid, :garbage_collect)

  Alternatively, you can configure your endpoint socket to trigger more
  fullsweep garbage collections more frequently, by setting the `:fullsweep_after`
  option for websockets. See `Phoenix.Endpoint.socket/3` for more info.

  ## Client-server communication

  The encoding of server data and the decoding of client data is done
  according to a serializer, defined in `Phoenix.Socket.Serializer`.
  By default, JSON encoding is used to broker messages to and from clients.

  The serializer `decode!` function must return a `Phoenix.Socket.Message`
  which is forwarded to channels except:

    * `"heartbeat"` events in the "phoenix" topic - should just emit an OK reply
    * `"phx_join"` on any topic - should join the topic
    * `"phx_leave"` on any topic - should leave the topic

  Each message also has a `ref` field which is used to track responses.

  The server may send messages or replies back. For messages, the
  ref uniquely identifies the message. For replies, the ref matches
  the original message. Both data-types also include a join_ref that
  uniquely identifies the currently joined channel.

  The `Phoenix.Socket` implementation may also send special messages
  and replies:

    * `"phx_error"` - in case of errors, such as a channel process
      crashing, or when attempting to join an already joined channel

    * `"phx_close"` - the channel was gracefully closed

  Phoenix ships with a JavaScript implementation of both websocket
  and long polling that interacts with Phoenix.Socket and can be
  used as reference for those interested in implementing custom clients.

  ## Custom sockets and transports

  See the `Phoenix.Socket.Transport` documentation for more information on
  writing your own socket that does not leverage channels or for writing
  your own transports that interacts with other sockets.

  ## Custom channels

  You can list any module as a channel as long as it implements
  a `child_spec/1` function. The `child_spec/1` function receives
  the caller as argument and it must return a child spec that
  initializes a process.

  Once the process is initialized, it will receive the following
  message:

      {Phoenix.Channel, auth_payload, from, socket}

  A custom channel implementation MUST invoke
  `GenServer.reply(from, {:ok | :error, reply_payload})` during its
  initialization with a custom `reply_payload` that will be sent as
  a reply to the client. Failing to do so will block the socket forever.

  A custom channel receives `Phoenix.Socket.Message` structs as regular
  messages from the transport. Replies to those messages and custom
  messages can be sent to the socket at any moment by building an
  appropriate `Phoenix.Socket.Reply` and `Phoenix.Socket.Message`
  structs, encoding them with the serializer and dispatching the
  serialized result to the transport.

  For example, to handle "phx_leave" messages, which is recommended
  to be handled by all channel implementations, one may do:

      def handle_info(
            %Message{topic: topic, event: "phx_leave"} = message,
            %{topic: topic, serializer: serializer, transport_pid: transport_pid} = socket
          ) do
        send transport_pid, serializer.encode!(build_leave_reply(message))
        {:stop, {:shutdown, :left}, socket}
      end

  A special message delivered to all channels is a Broadcast with
  event "phx_drain", which is sent when draining the socket during
  application shutdown. Typically it is handled by sending a drain
  message to the transport, causing it to shutdown:

      def handle_info(
            %Broadcast{event: "phx_drain"},
            %{transport_pid: transport_pid} = socket
          ) do
        send(transport_pid, :socket_drain)
        {:stop, {:shutdown, :draining}, socket}
      end

  We also recommend all channels to monitor the `transport_pid`
  on `init` and exit if the transport exits. We also advise to rewrite
  `:normal` exit reasons (usually due to the socket being closed)
  to the `{:shutdown, :closed}` to guarantee links are broken on
  the channel exit (as a `:normal` exit does not break links):

      def handle_info({:DOWN, _, _, transport_pid, reason}, %{transport_pid: transport_pid} = socket) do
        reason = if reason == :normal, do: {:shutdown, :closed}, else: reason
        {:stop, reason, socket}
      end

  Any process exit is treated as an error by the socket layer unless
  a `{:socket_close, pid, reason}` message is sent to the socket before
  shutdown.

  Custom channel implementations cannot be tested with `Phoenix.ChannelTest`.
  """

  require Logger
  alias Phoenix.Socket
  alias Phoenix.Socket.{Broadcast, Message, Reply}

  @doc """
  Receives the socket params and authenticates the connection.

  ## Socket params and assigns

  Socket params are passed from the client and can
  be used to verify and authenticate a user. After
  verification, you can put default assigns into
  the socket that will be set for all channels, ie

      {:ok, assign(socket, :user_id, verified_user_id)}

  To deny connection, return `:error` or `{:error, term}`. To control the
  response the client receives in that case, [define an error handler in the
  websocket
  configuration](https://hexdocs.pm/phoenix/Phoenix.Endpoint.html#socket/3-websocket-configuration).

  See `Phoenix.Token` documentation for examples in
  performing token verification on connect.
  """
  @callback connect(params :: map, Socket.t(), connect_info :: map) ::
              {:ok, Socket.t()} | {:error, term} | :error

  @doc """
  Shortcut version of `connect/3` which does not receive `connect_info`.

  Provided for backwards compatibility.
  """
  @callback connect(params :: map, Socket.t()) :: {:ok, Socket.t()} | {:error, term} | :error

  @doc ~S"""
  Identifies the socket connection.

  Socket IDs are topics that allow you to identify all sockets for a given user:

      def id(socket), do: "users_socket:#{socket.assigns.user_id}"

  Would allow you to broadcast a `"disconnect"` event and terminate
  all active sockets and channels for a given user:

      MyAppWeb.Endpoint.broadcast("users_socket:" <> user.id, "disconnect", %{})

  Returning `nil` makes this socket anonymous.
  """
  @callback id(Socket.t()) :: String.t() | nil

  @optional_callbacks connect: 2, connect: 3

  defmodule InvalidMessageError do
    @moduledoc """
    Raised when the socket message is invalid.
    """
    defexception [:message]
  end

  defstruct assigns: %{},
            channel: nil,
            channel_pid: nil,
            endpoint: nil,
            handler: nil,
            id: nil,
            joined: false,
            join_ref: nil,
            private: %{},
            pubsub_server: nil,
            ref: nil,
            serializer: nil,
            topic: nil,
            transport: nil,
            transport_pid: nil

  @type t :: %Socket{
          assigns: map,
          channel: atom,
          channel_pid: pid,
          endpoint: atom,
          handler: atom,
          id: String.t() | nil,
          joined: boolean,
          ref: term,
          private: map,
          pubsub_server: atom,
          serializer: atom,
          topic: String.t(),
          transport: atom,
          transport_pid: pid
        }

  defmacro __using__(opts)  do
    # ...
  end

  ## USER API

  @doc """
  Adds a `key`/`value` pair to `socket` assigns.

  See also `assign/2` to add multiple key/value pairs.

  ## Examples

      iex> assign(socket, :name, "Elixir")
  """
  def assign(%Socket{} = socket, key, value)  do
    # ...
  end

  @doc """
  Adds key/value pairs to socket assigns.
  Accepts a keyword list, a map, or a single-argument function.

  When a keyword list or map is provided, it will be merged into the existing assigns.

  If a function is given, it takes the current assigns as an argument and its return
  value will be merged into the current assigns.

  ## Examples

      iex> assign(socket, name: "Elixir", logo: "💧")
      iex> assign(socket, %{name: "Elixir"})
      iex> assign(socket, fn %{name: name, logo: logo} -> %{title: Enum.join([name, logo], " | ")} end)

  """
  def assign(%Socket{} = socket, keyword_or_map)
      when is_map(keyword_or_map) or is_list(keyword_or_map) do
    %{socket | assigns: Map.merge(socket.assigns, Map.new(keyword_or_map))}
  end

  def assign(%Socket{} = socket, fun) when is_function(fun, 1)  do
    # ...
  end

  @doc """
  Defines a channel matching the given topic and transports.

    * `topic_pattern` - The string pattern, for example `"room:*"`, `"users:*"`,
      or `"system"`
    * `module` - The channel module handler, for example `MyAppWeb.RoomChannel`
    * `opts` - The optional list of options, see below

  ## Options

    * `:assigns` - the map of socket assigns to merge into the socket on join

  ## Examples

      channel "topic1:*", MyChannel

  ## Topic Patterns

  The `channel` macro accepts topic patterns in two flavors. A splat (the `*`
  character) argument can be provided as the last character to indicate a
  `"topic:subtopic"` match. If a plain string is provided, only that topic will
  match the channel handler. Most use-cases will use the `"topic:*"` pattern to
  allow more versatile topic scoping.

  See `Phoenix.Channel` for more information
  """
  defmacro channel(topic_pattern, module, opts \\ [])  do
    # ...
  end

  ## CALLBACKS IMPLEMENTATION

  def __child_spec__(handler, opts, socket_options)  do
    # ...
  end

  def __drainer_spec__(handler, opts, socket_options)  do
    # ...
  end

  def __connect__(user_socket, map, socket_options)  do
    # ...
  end

  def __init__({state, %{id: id, endpoint: endpoint} = socket})  do
    # ...
  end

  def __in__({payload, opts}, {state, socket})  do
    # ...
  end

  def __info__({:DOWN, ref, _, pid, reason}, {state, socket})  do
    # ...
  end

  def __info__(%Broadcast{event: "disconnect"}, state)  do
    # ...
  end

  def __info__(:socket_drain, state)  do
    # ...
  end

  def __info__({:socket_push, opcode, payload}, state)  do
    # ...
  end

  def __info__({:socket_close, pid, _reason}, state)  do
    # ...
  end

  def __info__(:garbage_collect, state)  do
    # ...
  end

  def __info__({:debug_channels, ref, reply_to}, {state, socket})  do
    # ...
  end

  def __info__(_, state)  do
    # ...
  end

  def __terminate__(_reason, _state_socket)  do
    # ...
  end
end
```
