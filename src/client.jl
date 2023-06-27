"""
    Client([; host="127.0.0.1", port=6379, database=0, password="", username="", ssl_config=nothing, retry_when_closed=true, retry_max_attemps=1, retry_backoff=(x) -> 2^x, keepalive_enable=false, keepalive_delay=60]) -> Client

Creates a Client instance connecting and authenticating to a Redis host, provide an `MbedTLS.SSLConfig` 
(see `get_ssl_config`) for a secured Redis connection (SSL/TLS).

# Fields
- `host::AbstractString`: Redis host.
- `port::Integer`: Redis port.
- `database::Integer`: Redis database index.
- `password::AbstractString`: Redis password if any.
- `username::AbstractString`: Redis username if any.
- `socket::Union{TCPSocket,MbedTLS.SSLContext}`: Socket used for sending and reveiving from Redis host.
- `lock::Base.AbstractLock`: Lock for atomic reads and writes from client socket.
- `ssl_config::Union{MbedTLS.SSLConfig,Nothing}`: Optional ssl config for secured redis connection.
- `is_subscribed::Bool`: Whether this Client is actively subscribed to any channels or patterns.
- `subscriptions::AbstractSet{<:AbstractString}`: Set of channels currently subscribed on.
- `psubscriptions::AbstractSet{<:AbstractString}`: Set of patterns currently psubscribed on.
- `retry_when_closed::Bool`: Set `true` to try and reconnect when client socket status is closed, defaults to `true`.
- `retry_max_attemps`::Int`: Maximum number of retries for reconnection, defaults to `1`.
- `retry_backoff::Function`: Retry backoff function, called after each retry and must return a single number, where that number is the sleep time (in seconds) until the next retry, accepts a single argument, the number of retries attempted.
- `keepalive_enable::Bool=false`: Set `true` to enable TCP keep-alive.
- `keepalive_delay::Int=60`: Initial delay in seconds, defaults to 60s, ignored when `keepalive_enable` is `false`. After delay has been reached, 10 successive probes, each spaced 1 second from the previous one, will still happen. If the connection is still lost at the end of this procedure, then the handle is destroyed with a UV_ETIMEDOUT error passed to the corresponding callback.

# Note
- Connection parameters `host`, `port`, `database`, `password`, `username` will not change after 
client istance is constructed, even with `SELECT` or `CONFIG SET` commands.

# Examples
Basic connection:
```julia-repl
julia> client = Client();

julia> set("key", "value"; client=client)
"OK"

julia> get("key"; client=client)
"value"

julia> execute(["DEL", "key"], client)
1
```

SSL/TLS connection:
```julia-repl
julia> ssl_config = get_ssl_config(ssl_certfile="redis.crt", ssl_keyfile="redis.key", ssl_ca_certs="ca.crt");

julia> client = Client(ssl_config=ssl_config);
```
"""
mutable struct Client
    host::AbstractString
    port::Integer
    database::Integer
    password::AbstractString
    username::AbstractString
    socket::Union{TCPSocket,MbedTLS.SSLContext}
    lock::Base.AbstractLock
    ssl_config::Union{MbedTLS.SSLConfig,Nothing}
    is_subscribed::Bool
    subscriptions::AbstractSet{<:AbstractString}
    psubscriptions::AbstractSet{<:AbstractString}
    ssubscriptions::AbstractSet{<:AbstractString}
    retry_when_closed::Bool
    retry_max_attemps::Int
    retry_backoff::Function
    keepalive_enable::Bool
    keepalive_delay::Int
end

function Client(; host="127.0.0.1", port=6379, database=0, password="", username="", ssl_config=nothing, retry_when_closed=true, retry_max_attemps=1, retry_backoff=(x) -> 2^x, keepalive_enable=false, keepalive_delay=60)

    client = Client(
        host,
        port,
        database,
        password,
        username,
        isnothing(ssl_config) ? connect(host, port) : ssl_connect(host, port, ssl_config),
        ReentrantLock(),
        ssl_config,
        false,
        Set{String}(),
        Set{String}(),
        Set{String}(),
        retry_when_closed,
        retry_max_attemps,
        retry_backoff,
        keepalive_enable,
        keepalive_delay
    )
    prepare!(client)   
    
    return client
end

"""
    prepare!(client::Client)

Prepares a new client, involves pre-pinging the server, logging in with the correct username
and password, selecting the chosen database, and setting keepalive if applicable.

Pinging the server is to test connection and set socket status to Base.StatusPaused (i.e. a ready state).
Raw execution is used to bypass locks and retries
"""
function prepare!(client::Client)
    write(client.socket, resp(["PING"]))
    
    recv(client.socket)
    
    !isempty(client.password * client.username) && auth(client.password, client.username; client=client)
    client.database != 0 && select(client.database; client=client)

    client.keepalive_enable && keepalive!(client.socket, Cint(1), Cint(client.keepalive_delay))

    # Async garbage collect is needed to clear any stale clients
    @async GC.gc()
end

"""
    get_ssl_config([; ssl_certfile=nothing, ssl_keyfile=nothing, ssl_ca_certs=nothing]) -> MbedTLS.SSLConfig

Loads ssl cert, key and ca cert files from provided directories into MbedTLS.SSLConfig object.

# Examples
```julia-repl
julia> ssl_config = get_ssl_config(ssl_certfile="redis.crt", ssl_keyfile="redis.key", ssl_ca_certs="ca.crt");
```
"""
function get_ssl_config(; ssl_certfile=nothing, ssl_keyfile=nothing, ssl_ca_certs=nothing)
    ssl_config = MbedTLS.SSLConfig(false)

    if !isnothing(ssl_certfile) && !isnothing(ssl_keyfile)
        cert = MbedTLS.crt_parse_file(ssl_certfile)
        key = MbedTLS.parse_keyfile(ssl_keyfile)
        MbedTLS.own_cert!(ssl_config, cert, key)
    end

    if !isnothing(ssl_ca_certs)
        ca_certs = MbedTLS.crt_parse_file(ssl_ca_certs)
        MbedTLS.ca_chain!(ssl_config, ca_certs)
    end
    
    return ssl_config
end

"""
    ssl_connect(host::AbstractString, port::Integer, ssl_config::MbedTLS.SSLConfig) -> MbedTLS.SSLContext

Connects to the redis host and port, returns a socket connection with ssl context.
"""
function ssl_connect(host::AbstractString, port::Integer, ssl_config::MbedTLS.SSLConfig)
    tcp = connect(host, port)
    io = MbedTLS.SSLContext()
    MbedTLS.setup!(io, ssl_config)
    MbedTLS.associate!(io, tcp)
    MbedTLS.hostname!(io, host)
    MbedTLS.handshake!(io)
    return io
end

"""

"""
mutable struct Global_client
    clients::Dict{String, Any}
    cluster::Bool
    slots::Dict{Int, Vector{String}}
end

"""
    GLOBAL_CLIENT = Ref{GLOBAL_CLIENT}()

Reference to a Global Client object.
"""
const GLOBAL_CLIENT = Ref{Global_client}()

function update_client(client=Client, cluster=false, slots=Dict{Int, Vector{String}})
    
    cluster_check  = execute(["INFO", "CLUSTER"], client)

    if collect(split(collect(split(cluster_check, "\r"))[2], ":"))[2] == "1"
        @info "Cluster mode detected - configuring node connections for cluster mode"
        node_connections = configure_client_cluster(client)
        cluster = true

    else
        @info "Single instance Redis in use"
        node_connections = configure_client_single(client)
        cluster = false
    end
    update_slots(slots, node_connections)

    if  isdefined(GLOBAL_CLIENT, :x)
        nodes = GLOBAL_CLIENT[].clients
        nodes["node1"] = client
    else
        nodes = Dict("node2"=> client)
    end

    global_client = Global_client(
        node_connections,
        cluster,
        slots
    )
end

"""
    set_global_client(client::Client)
    set_global_client([; host="127.0.0.1", port=6379, database=0, password="", username="", ssl_config=nothing, retry_when_closed=true, retry_max_attemps=1, retry_backoff=(x) -> 2^x, keepalive_enable=false, keepalive_delay=60])

Sets a Client object as the `GLOBAL_CLIENT[]` instance.
"""
function set_global_client(client::Client, cluster::Bool, slots::Dict{Int, Vector{String}})
    # GLOBAL_CLIENT[] = client
    GLOBAL_CLIENT[] = update_client(client, cluster, slots)
end
function set_client(client::Client, cluster::Bool, slots::Dict{Int, Vector{String}})
    # GLOBAL_CLIENT[] = client
    update_client(client, cluster, slots)
end

function set_global_client(; host="127.0.0.1", port=6379, database=0, password="", username="", ssl_config=nothing, retry_when_closed=true, retry_max_attemps=1, retry_backoff=(x) -> 2^x, keepalive_enable=false, keepalive_delay=60)
    client = Client(; host=host, port=port, database=database, password=password, username=username, ssl_config=ssl_config, retry_when_closed=retry_when_closed, retry_max_attemps=retry_max_attemps, retry_backoff=retry_backoff, keepalive_enable=keepalive_enable, keepalive_delay=keepalive_delay)
    set_global_client(client, false, generate_slots())
end
function set_client_instance(; host="127.0.0.1", port=6379, database=0, password="", username="", ssl_config=nothing, retry_when_closed=true, retry_max_attemps=1, retry_backoff=(x) -> 2^x, keepalive_enable=false, keepalive_delay=60)
    client = Client(; host=host, port=port, database=database, password=password, username=username, ssl_config=ssl_config, retry_when_closed=retry_when_closed, retry_max_attemps=retry_max_attemps, retry_backoff=retry_backoff, keepalive_enable=keepalive_enable, keepalive_delay=keepalive_delay)
    Jedis.set_client(client, false, generate_slots())
end

"""
    get_global_client() -> GLOBAL_CLIENT

Retrieves the `GLOBAL_CLIENT[]` instance, if unassigned then initialises it with default values 
`host="127.0.0.1"`, `port=6379`, `database=0`, `password=""`, `username=""`.
"""
function get_global_client()
    if isassigned(GLOBAL_CLIENT)
        return GLOBAL_CLIENT[]
    else
        return set_global_client()
    end
end

"""
    endpoint(client::Client)

Retrieves the endpoint (host:port) of the client.
"""
function endpoint(client::Client)
    return "$(client.host):$(client.port)"
end

"""
    copy(client::Client) -> Client

Creates a new Client instance, copying the connection parameters of the input.
"""
function Base.copy(client::Client)
    return Client(;
        host=client.host,
        port=client.port,
        database=client.database,
        password=client.password,
        username=client.username,
        ssl_config=client.ssl_config,
        retry_when_closed=client.retry_when_closed,
        retry_max_attemps=client.retry_max_attemps,
        retry_backoff=client.retry_backoff,
        keepalive_enable=client.keepalive_enable,
        keepalive_delay=client.keepalive_delay
    )
end

"""
    disconnect!(client::Client)

Closes the client socket connection, it will be rendered unusable.
"""
function disconnect!(client::Client)
    close(client.socket)
end

"""
    reconnect!(client::Client) -> Client

Reconnects the input client socket connection.
"""
function reconnect!(client::Client)
    disconnect!(client)
    @info "Attempting to reconnect to $client"
    client.socket = isnothing(client.ssl_config) ? connect(client.host, client.port) : ssl_connect(connect(client.host, client.port), client.host, client.ssl_config)
    prepare!(client)
    return client
end
function reconnect!(client::Jedis.Global_client)
    for (_, node) in client.clients
        reconnect!(node["client"])
    end
    return client
end

"""
    flush!(client::Client)

Reads and discards any bytes that remain unread in the client socket.
"""
function flush!(client::Client)
    nb = bytesavailable(client.socket)
    if nb > 0
        buffer = Vector{UInt8}(undef, nb)
        readbytes!(client.socket, buffer, nb)
    end
end
function flush!(client::Jedis.Global_client)
    if typeof(client) == Client
        nb = bytesavailable(client.socket)
        if nb > 0
            buffer = Vector{UInt8}(undef, nb)
            readbytes!(client.socket, buffer, nb)
        end
    end

    if typeof(client) == GLOBAL_CLIENT
        for (_, c) in client.clients
            nb = bytesavailable(c["client"].socket)
            if nb > 0
                buffer = Vector{UInt8}(undef, nb)
                readbytes!(c["client"].socket, buffer, nb)
            end
        end
    end
end

"""
    status(client::Client)

Returns the status of the client socket.
"""
function status(client::Client)
    if client.socket isa TCPSocket
        return client.socket.status
    elseif client.socket isa MbedTLS.SSLContext
        return client.socket.bio.status
    else
        throw(RedisError("INVALIDSOCKET", "Invalid socket type: $(typeof(client.socket))"))
    end
end

"""
    isclosed(client::Client)

Returns `true` if client socket status is `Base.StatusClosing`, `Base.StatusClosed` or 
`Base.StatusOpen`, `false` otherwise. It turns out when status is `Base.StatusOpen` the socket 
is already unusable. `Base.StatusPaused` is the true ready state.
"""
function isclosed(client::Client)
    return status(client) == Base.StatusClosing || status(client) == Base.StatusClosed || status(client) == Base.StatusOpen
end
function isclosed(client::Global_client)
    closed = []
    for (key, node) in client.clients
        push!(closed, status(node["client"]) == Base.StatusClosing || status(node["client"]) == Base.StatusClosed || status(node["client"]) == Base.StatusOpen)
    end
    return  all(closed) 
end

"""
    retry!(client::Client)

Attempts to re-estiablish client socket connection, behaviour is determined by the retry parameters;
`retry_when_closed`, `retry_max_attemps`, `retry_backoff`.
"""
function retry!(client::Client)
    if !isclosed(client)
        return
    end

    if !client.retry_when_closed
        throw(Base.IOError("Client connection to $(endpoint(client)) is closed or unusable, try establishing a new connection, or set `retry_when_closed` field to `true`", Base.StatusUninit))
    end

    @warn "Client socket is closed or unusable, retrying connection to $(endpoint(client))"
    attempts = 0

    while attempts < client.retry_max_attemps
        attempts += 1
        @info "Reconnection attempt #$attempts to $(endpoint(client))"

        try
            reconnect!(client)
            @info "Reconnection attempt #$attempts to $(endpoint(client)) was successful"
            return
        catch err
            if !(err isa Base.IOError)
                rethrow()
            end

            @warn "Reconnection attempt #$attempts to $(endpoint(client)) was unsuccessful"
        end

        if attempts < client.retry_max_attemps
            backoff = client.retry_backoff(attempts)
            @info "Sleeping $(backoff)s until next reconnection attempt to $(endpoint(client))"
            sleep(backoff)
        end
    end

    throw(Base.IOError("Client connection to $(endpoint(client)) is closed or unusable, try establishing a new connection, or set `retry_when_closed` field to `true`", Base.StatusUninit))
end

"""
    set_subscribed!(client::Client)

Marks the Client instance as subscribed, should not be used publicly.
"""
function set_subscribed!(client::Client)
    client.is_subscribed = true
end

"""
    set_unsubscribed!(client::Client)

Marks the Client instance as unsubscribed, should not be used publicly.
"""
function set_unsubscribed!(client::Client)
    client.is_subscribed = false
end

"""
    wait_until_subscribed(client::Client)

Blocks until client changes to a subscribed state.
"""
function wait_until_subscribed(client::Client)
    if !client.is_subscribed
        while !client.is_subscribed
            sleep(0.001)
        end
    end
end

"""
    wait_until_unsubscribed(client::Client)

Blocks until client changes to a unsubscribed state.
"""
function wait_until_unsubscribed(client::Client)
    if client.is_subscribed
        while client.is_subscribed
            sleep(0.001)
        end
    end
end

"""
    wait_until_channel_unsubscribed(client::Client[, channels...])

Blocks until client is unsubscribed from channel(s), leave empty to wait until unsubscribed from all channels.
"""
function wait_until_channel_unsubscribed(client::Client, channels...)
    if isempty(channels)
        while !isempty(client.subscriptions)
            sleep(0.001)
        end
    else
        while !isempty(intersect(client.subscriptions, Set{String}(channels)))
            sleep(0.001)
        end
    end
end

"""
    wait_until_pattern_unsubscribed(client::Client[, patterns...])

Blocks until client is unsubscribed from pattern(s), leave empty to wait until unsubscribed from all patterns.
"""
function wait_until_pattern_unsubscribed(client::Client, patterns...)
    if isempty(patterns)
        while !isempty(client.psubscriptions)
            sleep(0.001)
        end
    else
        while !isempty(intersect(client.psubscriptions, Set{String}(patterns)))
            sleep(0.001)
        end
    end
end

"""
    wait_until_shard_unsubscribed(client::Client[, shards...])

Blocks until client is unsubscribed from shard(s), leave empty to wait until unsubscribed from all shards.
"""
function wait_until_shard_unsubscribed(client::Client, shards...)
    if isempty(shards)
        while !isempty(client.ssubscriptions)
            sleep(0.001)
        end
    else
        while !isempty(intersect(client.ssubscriptions, Set{String}(shards)))
            sleep(0.001)
        end
    end
end

function configure_client_single(client::Client)
    node_connections = Dict()  
    node_connections["instance"] = Dict()
    node_connections["instance"]["port"] = client.port
    node_connections["instance"]["host"] = client.host
    node_connections["instance"]["node"] = "instance"
    node_connections["instance"]["start_slot"] = 1
    node_connections["instance"]["end_slot"] = 16384
    node_connections["instance"]["node_type"] = "primary"

    node_connections["instance"]["client"] =  Client(
        port = node_connections["instance"]["port"],
        host = node_connections["instance"]["host"],
        database=client.database,
        password=client.password,
        username=client.username,
        ssl_config=client.ssl_config,
        retry_when_closed=client.retry_when_closed,
        retry_max_attemps=client.retry_max_attemps,
        retry_backoff=client.retry_backoff,
        keepalive_enable=client.keepalive_enable,
        keepalive_delay=client.keepalive_delay
    )

    @info "Establishing connection with: 
            port:$(node_connections["instance"]["port"]) 
            host:$(node_connections["instance"]["host"]) 
            type:$(node_connections["instance"]["node_type"])"
    return node_connections
end

function configure_client_cluster(client::Client)
    cluster_config  = execute(["CLUSTER", "SLOTS"], client)
    node_count = 0
    node_connections = Dict()  
    for shard in cluster_config
        for (i, node) in enumerate(shard[3:end,:])
            node_count += 1
            node_connections[node[3]] = Dict()
            node_connections[node[3]]["port"] = node[2]
            node_connections[node[3]]["host"] = node[1]
            node_connections[node[3]]["node"] = node[3]
            node_connections[node[3]]["start_slot"] = shard[1]
            node_connections[node[3]]["end_slot"] = shard[2]
            node_connections[node[3]]["client"] =  Client(
                port = node_connections[node[3]]["port"],
                host = node_connections[node[3]]["host"],
                database=client.database,
                password=client.password,
                username=client.username,
                ssl_config=client.ssl_config,
                retry_when_closed=client.retry_when_closed,
                retry_max_attemps=client.retry_max_attemps,
                retry_backoff=client.retry_backoff,
                keepalive_enable=client.keepalive_enable,
                keepalive_delay=client.keepalive_delay
            )
            if i == 1
                node_connections[node[3]]["node_type"] = "primary"
            else
                node_connections[node[3]]["node_type"] = "slave"
            end
            @info "Establishing connection with:
                    port:$(node_connections[node[3]]["port"]) 
                    host:$(node_connections[node[3]]["host"]) 
                    type:$(node_connections[node[3]]["node_type"])"
        end
    end
    @info "Connected to $node_count nodes in cluster"
    return node_connections
end



"""
    generate_slots()

Generate slots for node allocation.
    returns a Dict with slots as keys and array of nodes as values - this is initalised as empty
"""
function generate_slots()
    total_slots = 16384
    slots = Dict(0 => String[])
        for slot in 1:total_slots
            slots[slot] = String[]
        end
    return slots
end


"""
    update_slots(slots::Dict{Int, Vector{String}}, node_connections)

Generate slots for node allocation per client.
    returns a Dict with slots as keys and array of nodes as values - this is initalised as empty
"""
function update_slots(slots::Dict{Int, Vector{String}}, node_connections)

    @info "Pre-allocating slots to nodes"
    for (key, node) in node_connections
        for slot in collect(node["start_slot"]:node["end_slot"])
            push!(slots[slot], node["node"])
        end
    end
    for (key, slot) in slots
        primary = ""
        slaves = []
        for node in slot
            if node_connections[node]["node_type"] == "primary"
                primary = node_connections[node]["node"]
            else
                push!(slaves, node_connections[node]["node"])
            end
        end
        slots[key] = vcat(primary,slaves)
    end
    @info "Slot allocation established"
end

const CRC16_TABLE = Ref{CRC.var"#handler#7"{CRC.var"#handler#4#8"{Multiple{UInt64}, CRC.Spec{UInt16}, CRC.Forwards{UInt64}, DataType}}}(crc(CRC_16_XMODEM))

function get_hash_slot(key::String)
    hash_temp = CRC16_TABLE[](key)
    slot = mod(hash_temp, 16384)
    return slot
end

function get_hash_key(key::String)
    a = findfirst("{", key)[1]
    return collect(split(key[a+1:end],"}"))[1]
end

function get_client(client::Jedis.Global_client, keys::Vector{String}, write::Bool=false, replica::Bool=false)
    if keys[1] == "*" && !write
        # @info "Subscribe or publish to any node"
        node = rand(client.clients)[1]
    elseif keys[1] == "*" && write
        # @info "Subscribe or publish to a primary node"
        node = get_any_primary_node(client)
    else
        slots = []
        for key::String in keys
            if occursin("{", key) && occursin("}", key)
                key = get_hash_key(key)
            end

            push!(slots, key)
        end
        allequal(x) = all(y -> y == x[1], x)
        if allequal(slots)
            slot = get_hash_slot(slots[1])
            if ~write && replica
                @info "Redirecting to replica"
                node = rand(client.slots[slot][2:end])
                execute(["READONLY"], client.clients[node]["client"])
            else
                node = client.slots[slot][1]
            end
        else 
            throw(RedisError("CROSSSLOT", "Keys in request don't hash to the same slot"))
        end
        
    end
    client_connection = client.clients[node]["client"]
    return client_connection
end

function get_client(client::Client, keys::Vector{String}, write::Bool=false, replica::Bool=false)
    return client
end

function get_any_primary_node(client)
    (key, node) = rand(client.clients)
    while node["node_type"] != "primary"
        (key, node) = rand(client.clients)
    end
    return key
end

function get_primary_nodes(client)
    nodes = []
    for (key, node) in client.clients
        if node["node_type"] == "primary"
            push!(nodes, node)
        end
    end
    return nodes
end
function get_all_nodes(client)
    nodes = []
    for (key, node) in client.clients
        push!(nodes, node)
    end
    return nodes
end