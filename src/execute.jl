"""
    execute(command[; client::Client=get_global_client()])

Sends a RESP compliant command to the Redis host and returns the result. The command is either an 
array of command keywords, or a single command string. Defaults to using the globally set Client.

# Examples
```julia-repl
julia> execute(["SET", "key", "value"])
"OK"
julia> execute("GET key")
"value"
```
"""
function execute(command::AbstractArray, client::Client)
    if client.is_subscribed
        throw(RedisError("SUBERROR", "Cannot execute commands while a subscription is open in the same Client instance"))
    end

    @lock client.lock begin
        flush!(client)
        retry!(client)
        write(client.socket, resp(command))
        msg = recv(client.socket)
        print(msg)
        
        if msg isa Exception
            throw(msg)
        end

        return msg
    end
end
function execute(command::AbstractString, client::Client)
    execute(split_on_whitespace(command), client)
end

"""
    execute_without_recv(command[; client::Client=get_global_client()])

Sends a RESP compliant command to the Redis host without reading the returned result.
"""
function execute_without_recv(command::AbstractArray, client)
    @lock client.lock begin
        flush!(client)
        retry!(client)
        write(client.socket, resp(command))
        return
    end
end
function execute_without_recv(command::AbstractString, client::Client)
    execute_without_recv(split_on_whitespace(command), client)
end

"""
    execute(command, pipe::Pipeline)

Add a RESP compliant command to a pipeline client, optionally filter out MULTI transaction responses
before the EXEC call, e.g. "QUEUED".

# Examples
```julia-repl
julia> pipe = Pipeline();

julia> execute(["SET", "key", "value"]; client=pipe);

julia> execute(["GET", "key"]; client=pipe);

julia> execute(pipe)
2-element Array{String,1}:
 "OK"
 "value"
```
"""
function execute(command::AbstractArray, pipe::Pipeline)
    add!(pipe, command)
    return
end
function execute(command::AbstractString, pipe::Pipeline)
    execute(split_on_whitespace(command), pipe)
end

"""
    execute(pipe::Pipeline[, batch_size::Int; filter_multi_exec=true])

Execute commands batched in a pipeline client, optionally filter out MULTI transaction responses
before the EXEC call, e.g. "QUEUED". Set `batch_size` to batch commands with max commands 
per pipeline, defaults to use a single pipeline for all commands.

# Examples
```julia-repl
julia> pipe = Pipeline();

julia> set("key", "value"; client=pipe);

julia> get("key"; client=pipe);

julia> multi(; client=pipe);

julia> get("key"; client=pipe);

julia> get("key"; client=pipe);

julia> exec(; client=pipe);

julia> execute(pipe)
2-element Array{String,1}:
 "OK"
 "value"
 ["value", "value"]  # Only the response from final exec() call is returned
```
"""
function execute(pipe::Pipeline)

    messages = []
    order = []
    if pipe.filter_multi_exec == true
        # Filter first and last element
        pipe.client_exec = pipe.client_exec[2:end-1]
        pipe.order = pipe.order[2:end-1]
    end

    try
        for client in unique(pipe.client_exec)
            node = pipe.client.clients[client]["client"]
            resp = pipe.resp[pipe.client_exec .== client]

            if node.is_subscribed
                throw(RedisError("SUBERROR", "Cannot execute Pipeline while a subscription is open in the same Client instance"))
            end

            @lock node.lock begin
                    flush!(node)
                    retry!(node)
                    write(node.socket, join(resp))
                    append!(messages, recv(node.socket) for _ in 1:length(resp))
                    append!(order, pipe.order[pipe.client_exec .== client])
            end
        end

        # sort responses to match execution order
    responses = hcat(order, messages)
    responses = responses[sortperm(responses[:, 1]), :]
    messages = responses[:,2]
    if pipe.filter_multi_exec
        return messages[pipe.multi_exec_bitmask]
    end
    return messages
   
    finally
        flush!(pipe)
    end
end            
function execute(pipe::Pipeline, batch_size::Int)
    
    messages = []
    order = []
    try
        for client in unique(pipe.client_exec)

            node = pipe.client.clients[client]["client"]
            resp = pipe.resp[pipe.client_exec .== client]
            node_order = pipe.order[pipe.client_exec .== client]

            if node.is_subscribed
                throw(RedisError("SUBERROR", "Cannot execute Pipeline while a subscription is open in the same Client instance"))
            end

            @lock node.lock begin
            
                flush!(node)
                retry!(node)

                n_cmd = length(resp)
                l, r  = 1, batch_size
                
                while l <= n_cmd
                    write(node.socket, join(resp[l:min(r, n_cmd)]))
                    
                    for i in l:min(r, n_cmd)
                        push!(messages, recv(node.socket))
                    end

                    if r > length(node_order)
                        append!(order, node_order[l:end])
                    else
                        append!(order, node_order[l:r])
                    end
                    
                    l += batch_size
                    r += batch_size
                end
                
            end
            
        end
    responses = hcat(order, messages)
    responses = responses[sortperm(responses[:, 1]), :]
    messages = responses[:,2]

    if pipe.filter_multi_exec
        return messages[pipe.multi_exec_bitmask]
    end
    
    return messages

    finally
        flush!(pipe)
    end
    
end      