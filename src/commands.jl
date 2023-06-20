function run_on_all_primary_nodes(fn::Function, client::Jedis.Global_client)
    nodes = get_primary_nodes(client)
    responses = []
    for node in nodes       
        push!(responses, response_format(fn(node["client"]))...)
    end
    if  allsame(responses) && ~isempty(responses) && (responses[1] == "OK" || responses[1] == "PONG")
        responses = responses[1]
    end

    return responses
end
function run_on_all_primary_nodes(fn::Function, client::Client)
    return fn(client)
end
function run_on_all_nodes(fn::Function, client::Jedis.Global_client)
    nodes = get_all_nodes(client)
    responses = []
    for node in nodes       
        push!(responses, response_format(fn(node["client"]))...)
    end
    if  allsame(responses) && ~isempty(responses) && (responses[1] == "OK" || responses[1] == "PONG")
        responses = responses[1]
    end

    return responses
end
function run_on_all_nodes(fn::Function, client::Client)
    return fn(client)
end
allsame(x) = all(y -> y == first(x), x)
function response_format(a::String)
    # code for handling a single Int
    return [a]
end
function response_format(a::Vector{Any})
     # code handling a vector containing type Any
    return a
end
function response_format(a::Vector{String})
    # code handling a vector containing only Strings
    return a
end

"""
    auth(password[, username])

Authenticate to the server.
"""
auth(password, username=""; client=get_global_client()) = Jedis.run_on_all_primary_nodes(client) do node_client
    execute(["AUTH", username, password], node_client)
end

"""
    select(database)

Change the selected database for the current connection.
"""
select(database; client=get_global_client()) = Jedis.run_on_all_primary_nodes(client) do node_client
    execute(["SELECT", database], get_client(node_client, ["*"], true, false))
end
"""
    ping()

Ping the server.
"""
ping(; client=get_global_client()) = Jedis.run_on_all_primary_nodes(client) do node_client
    execute(["PING"], get_client(node_client, ["*"], true, false))
end

"""
    flushdb([; async=false])

Remove all keys from the current database.
"""
flushdb(; async=false, client=get_global_client()) = Jedis.run_on_all_primary_nodes(client) do node_client
    execute(["FLUSHDB", async ? "ASYNC" : ""], node_client)
end

"""
    flushall([; async=false])

Remove all keys from all databases.
"""
flushall(; async=false, client=get_global_client()) = Jedis.run_on_all_primary_nodes(client) do node_client
    execute(["FLUSHALL", async ? "ASYNC" : ""], node_client)
end

"""
    quit()

Close the connection.
"""
quit(; client=get_global_client()) = Jedis.run_on_all_nodes(client) do node_client
    execute(["QUIT"], node_client)
end

"""
    set(key, value)

Set the string value of a key.
"""
set(key, value; client=get_global_client()) = execute(["SET", key, value], get_client(client, [key], true, false))

"""
    setnx(key, value)

Set the value of a key, only if the key does not exist.
"""
setnx(key, value; client=get_global_client()) = execute(["SETNX", key, value], get_client(client, [key], true, false))

"""
    get(key)

Get the value of a key.
"""
Base.get(key; client=get_global_client()) = execute(["GET", key], get_client(client, [key], false, false))

"""
    del(key[, keys...])

Delete a key.
"""
del(key, keys...; client=get_global_client()) = mdel([key, keys...], client=client) 
function mdel(keys; client=get_global_client())
    responses = []
    if length(keys) > 1
        for key in keys
            push!(responses, execute(["DEL", key],  Jedis.get_client(client, [key], false, false)))
        end
    else
        key = keys[1]
        responses = execute(["DEL", key],  Jedis.get_client(client, [key], false, false))
    end
    return responses
end


"""
    exists(key[, keys...])

Determine if a key exists.
"""
exists(key, keys...; client=get_global_client()) = execute(["EXISTS", key, keys...],  Jedis.get_client(client, [key], false, false))

"""
    hexists(key, field)

Determine if a hash field exists.
"""
hexists(key, field; client=get_global_client()) = execute(["HEXISTS", key, field], Jedis.get_client(client, key, true))

"""
    keys(pattern)

Find all keys matching the pattern.

!!! compat "Julia 1.6"
    `Jedis.keys` is not exported as the interface conflicts with `Base.keys`.
"""
keys(pattern; client=get_global_client()) = Jedis.run_on_all_primary_nodes(client) do node_client
        execute(["KEYS", pattern], node_client)
end

"""
    hkeys(key)

Get all fields in a hash.
"""
hkeys(key; client=get_global_client()) = execute(["HKEYS", key], get_client(client, ["*"], true, false))

"""
    setex(key, seconds, value)

Set the value and expiration of a key.
"""
setex(key, seconds, value; client=get_global_client()) = execute(["SETEX", key, seconds, value], get_client(client, [key], true, false))

"""
    expire(key, seconds)

Set a key's tiem to live in seconds.
"""
expire(key, seconds; client=get_global_client()) = execute(["EXPIRE", key, seconds], get_client(client, [key], true, false))

"""
    ttl(key)

Get the time to live for a key.
"""
ttl(key; client=get_global_client()) = execute(["TTL", key], get_client(client, [key], false, false))

"""
    multi()

Mark the start of a transaction block.

# Examples
```julia-repl
julia> multi()
"OK"

julia> set("key", "value")
"QUEUED"

julia> get("key")
"QUEUED"

julia> exec()
2-element Array{String,1}:
 "OK"
 "value"
```
"""
multi(; client=get_global_client()) = execute(["MULTI"], get_client(client, ["*"], true, false))

"""
    exec()

Execute all commands issued after MULTI.

# Examples
```julia-repl
julia> multi()
"OK"

julia> set("key", "value")
"QUEUED"

julia> get("key")
"QUEUED"

julia> exec()
2-element Array{String,1}:
 "OK"
 "value"
```
"""
exec(; client=get_global_client()) = execute(["EXEC"], get_client(client, ["*"], true, false))

"""
    multi_exec(fn::Function)

Execute a MULTI/EXEC transction in a do block.

# Examples
```julia-repl
julia> multi_exec() do 
           set("key", "value")
           get("key")
           get("key")
       end
3-element Array{String,1}:
 "OK"
 "value"
 "value"
```
"""
multi_exec(fn::Function; client=get_global_client()) = (multi(; client=get_client(client, ["*"], true, false)); fn(); exec(; client=get_client(client, ["*"], true, false)))


"""
    hset(key, field, value)

Set the string value of a hash field.
"""
hset(key, field, value, fields_and_values...; client=get_global_client()) = execute(["HSET", key, field, value, fields_and_values...], get_client(client, [key], true, false))

"""
    hget(key, field)

Get the value of a hash field.
"""
hget(key, field; client=get_global_client()) = execute(["HGET", key, field], get_client(client, [key], false, false))

"""
    hgetall(key)

Get all the fields and values in a hash.
"""
hgetall(key; client=get_global_client()) = execute(["HGETALL", key], Jedis.get_client(client, [key], false, false))

"""
    hmget(key, field[, fields...])

Get the values of all the given hash fields.
"""
hmget(key, field, fields...; client=get_global_client()) = execute(["HMGET", key, field, fields...], Jedis.get_client(client, [key], false, false))

"""
    hdel(key, field[, fields...])

Delete one or more hash fields.
"""
hdel(key, field, fields...; client=get_global_client()) = execute(["HDEL", key, field, fields...], Jedis.get_client(client, [key], true, false))

"""
    lpush(key, element[, elements...])

Prepend one or multiple elements to a list.
"""
lpush(key, element, elements...; client=get_global_client()) = execute(["LPUSH", key, element, elements...], Jedis.get_client(client, [key], true, false))

"""
    rpush(key, element[, elements...])

Append one or multiple elements to a list.
"""
rpush(key, element, elements...; client=get_global_client()) = execute(["RPUSH", key, element, elements...], Jedis.get_client(client, [key], false, false))

"""
    lpos(key, element[, rank, num_matches, len])

Return the index of matching element on a list.
"""
lpos(key, element, rank="", num_matches="", len=""; client=get_global_client()) = execute(["LPOS", key, element, [isempty(rank) ? "" : "RANK", rank]..., [isempty(num_matches) ? "" : "COUNT", num_matches]..., [isempty(len) ? "" : "MAXLEN", len]...], Jedis.get_client(client, [key], false, false))

"""
    lrem(key, count, element)

Remove elements from a list.
"""
lrem(key, count, element; client=get_global_client()) = execute(["LREM", key, count, element], Jedis.get_client(client, [key], true, false))

"""
    lpop(key)

Remove and get the first element in a list.
"""
lpop(key; client=get_global_client()) = execute(["LPOP", key], Jedis.get_client(client, [key], false, false))

"""
    rpop(key)

Remove and get the last element in a list.
"""
rpop(key; client=get_global_client()) = execute(["RPOP", key], Jedis.get_client(client, [key], false, false))

"""
    blpop(key[, key...; timeout=0])

Remove and get the first element in a list, or block until one is available.
"""
blpop(key, keys...; timeout=0, client=get_global_client()) = execute(["BLPOP", key, keys..., timeout],  Jedis.get_client(client, [key], false, false))

"""
    brpop(key[, key...; timeout=0])

Remove and get the last element in a list, or block until one is available.
"""
brpop(key, keys...; timeout=0, client=get_global_client()) = execute(["BRPOP", key, keys..., timeout], Jedis.get_client(client, [key], false, false))

"""
    llen(key)

Get the length of a list.
"""
llen(key; client=get_global_client()) = execute(["LLEN", key],  Jedis.get_client(client, [key], false, false))

"""
    lrange(key, start, stop)

Get a range of elements from a list.
"""
lrange(key, start, stop; client=get_global_client()) = execute(["LRANGE", key, start, stop], Jedis.get_client(client, [key], false, false))

"""
    incr(key)

Increment the integer value of a key by one.
"""
incr(key; client=get_global_client()) = execute(["INCR", key], Jedis.get_client(client, [key], true, false))

"""
    incrby(key, increment)

Increment the integer value of a key by the given amount.
"""
incrby(key, increment; client=get_global_client()) = execute(["INCRBY", key, increment], Jedis.get_client(client, [key], true, false))

"""
    incrbyfloat(key, increment)

Increment the float value of a key by the given amount.
"""
incrbyfloat(key, increment; client=get_global_client()) = execute(["INCRBYFLOAT", key, increment], Jedis.get_client(client, [key], true, false))

"""
    hincrby(key, field, increment)

Increment the integer value of a hash field by the given number.
"""
hincrby(key, increment; client=get_global_client()) = execute(["HINCRBY", key, field, increment], Jedis.get_client(client, [key], true, false))

"""
    hincrbyfloat(key, field, increment)

Increment the float value of a hash field by the given number.
"""
hincrbyfloat(key, field, increment; client=get_global_client()) = execute(["HINCRBYFLOAT", key, field, increment], Jedis.get_client(client, [key], true, false))

"""
    zincrby(key, field, member)

Increment the score of a member in a sorted set.
"""
zincrby(key, field, increment; client=get_global_client()) = execute(["ZINCRBY", key, field, increment], Jedis.get_client(client, [key], true, false))

"""
    zadd(key, score, member[, score_and_members...])

Add one or more members to a sorted set, or update its score if it 
already exists.
"""
zadd(key, score, member, score_and_members...; client=get_global_client()) = execute(["ZADD", key, score, member, score_and_members...], Jedis.get_client(client, [key], true, false))

"""
    zrange(key, min, max)

Store a range of members from sorted set into another key.
"""
zrange(key, min, max; client=get_global_client()) = execute(["ZRANGE", key, min, max], Jedis.get_client(client, [key], true, false))

"""
    zrangebyscore(key, min, max)

Return a range of members in a sorted set, by score.
"""
zrangebyscore(key, min, max; client=get_global_client()) = execute(["ZRANGEBYSCORE", key, min, max], Jedis.get_client(client, [key], true, false))

"""
    zrem(key, member[, members...])

Remove one or more members from a sorted set.
"""
zrem(key, member, members...; client=get_global_client()) = execute(["ZREM", key, member, members...], Jedis.get_client(client, [key], true, false))

