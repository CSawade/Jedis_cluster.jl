set_global_client()

@testset "SUBSCRIBE" begin
    channels = ["first", "second", "third"]
    publisher = set_global_client()
    subscriber = Client()
    messages = []
    @test subscriber.is_subscribed == false
    
    @async subscribe(channels..., client=subscriber) do msg
        push!(messages, msg)
    end
    
    wait_until_subscribed(subscriber)
    @test subscriber.is_subscribed == true
    @test subscriber.subscriptions == Set{String}(channels)
    
    # Redis cluster may not return the correct number of channels
    if Jedis.GLOBAL_CLIENT[].cluster == false
        @test publish("first", "hello"; client=publisher) == 1
        @test publish("second", "world"; client=publisher) == 1
        @test publish("something", "else"; client=publisher) == 0
    else
        @test publish("first", "hello"; client=publisher) >= 0
        @test publish("second", "world"; client=publisher) >= 0
        @test publish("something", "else"; client=publisher) == 0
    end
    
    @test length(messages) == 2
    @test messages[1] == ["message", "first", "hello"]
    @test messages[2] == ["message", "second", "world"]
    
    @test_throws RedisError set("already", "subscribed"; client=subscriber)
    @test_throws RedisError subscribe("alreadsubscribed"; client=subscriber) do msg end
    
    unsubscribe("first"; client=subscriber)
    
    wait_until_channel_unsubscribed(subscriber, "first")
    @test subscriber.is_subscribed == true
    @test subscriber.subscriptions == Set{String}(["second", "third"])
    
    @test publish("first", "not subscribed anymore"; client=publisher) == 0
    
    @test length(messages) == 2
    
    unsubscribe(; client=subscriber) # unsubscribe from everything
    
    wait_until_unsubscribed(subscriber)
    @test subscriber.is_subscribed == false
    @test isempty(subscriber.subscriptions)
    
    stop_fn(msg) = msg[end] == "close subscription"
    
    @async subscribe(channels...; stop_fn=stop_fn, client=subscriber) do msg end
    
    wait_until_subscribed(subscriber)
    @test subscriber.is_subscribed == true
    @test subscriber.subscriptions == Set{String}(channels)
    
    # Redis cluster may not return the correct number of channels
    if Jedis.GLOBAL_CLIENT[].cluster == false
        @test publish("first", "close subscription"; client=publisher) == 1
    else
        @test publish("first", "close subscription"; client=publisher) >= 0
    end
    
    wait_until_unsubscribed(subscriber)
    @test subscriber.is_subscribed == false
    @test isempty(subscriber.subscriptions)
end

@testset "PSUBSCRIBE" begin
    patterns = ["first*", "second*", "third*"]
    publisher = Client()
    subscriber = Client()
    messages = []
    @test subscriber.is_subscribed == false
    
    @async psubscribe(patterns...; client=subscriber) do msg
        push!(messages, msg)
    end
    
    wait_until_subscribed(subscriber)
    @test subscriber.is_subscribed == true
    @test subscriber.psubscriptions == Set{String}(patterns)
    
    @test publish("first_pattern", "hello"; client=publisher) == 1
    @test publish("second_pattern", "world"; client=publisher) == 1
    @test publish("something", "else"; client=publisher) == 0
    
    @test length(messages) == 2
    @test messages[1] == ["pmessage", "first*", "first_pattern", "hello"]
    @test messages[2] == ["pmessage", "second*", "second_pattern", "world"]
    
    @test_throws RedisError set("already", "subscribed"; client=subscriber)
    @test_throws RedisError psubscribe("alreadsubscribed"; client=subscriber) do msg end
    
    punsubscribe("first*"; client=subscriber)
    
    wait_until_pattern_unsubscribed(subscriber, "first*")
    @test subscriber.is_subscribed == true
    @test subscriber.psubscriptions == Set{String}(["second*", "third*"])
    
    @test publish("first_pattern", "not subscribed anymore"; client=publisher) == 0
    
    @test length(messages) == 2
    
    punsubscribe(; client=subscriber) # unsubscribe from everything
    
    wait_until_unsubscribed(subscriber)
    @test subscriber.is_subscribed == false
    @test isempty(subscriber.psubscriptions)
    
    stop_fn(msg) = msg[end] == "close subscription"
    
    @async psubscribe(patterns...; stop_fn=stop_fn, client=subscriber) do msg end
    
    wait_until_subscribed(subscriber)
    @test subscriber.is_subscribed == true
    @test subscriber.psubscriptions == Set{String}(patterns)
    
    @test publish("first_pattern", "close subscription"; client=publisher) == 1
    
    wait_until_unsubscribed(subscriber)
    @test subscriber.is_subscribed == false
    @test isempty(subscriber.subscriptions)

    task = @async psubscribe(patterns...; client=subscriber) do msg
        push!(messages, msg)
    end
    
    wait_until_subscribed(subscriber)
    disconnect!(subscriber)  # force close
    
    @test subscriber.is_subscribed == false
    @test isempty(subscriber.subscriptions)
    @test istaskdone(task)
    try
        fetch(task)
    catch err
        @test err isa TaskFailedException
        @test err.task.result isa Base.IOError
        @test err.task.result.code == Base.UV_ECONNABORTED
    end
end

@testset "SSUBSCRIBE" begin
    if Jedis.GLOBAL_CLIENT[].cluster == false
        channels = ["first", "second", "third"]
    else
        channels = ["{shard}:first", "{shard}:second", "{shard}:third"]
    end
    publisher = Client()
    subscriber = Client()
    messages = []
    @test subscriber.is_subscribed == false
    
    @async ssubscribe(channels...; client=subscriber) do msg
        push!(messages, msg)
    end
    
    wait_until_subscribed(subscriber)
    @test subscriber.is_subscribed == true
    @test subscriber.ssubscriptions == Set{String}(channels)
    
    @test spublish(channels[1], "hello"; client=publisher) == 1
    @test spublish(channels[2], "world"; client=publisher) == 1
    @test spublish("{shard}:something", "else"; client=publisher) == 0

    @test length(messages) == 2
    @test messages[1] == ["smessage", channels[1], "hello"]
    @test messages[2] == ["smessage", channels[2], "world"]
    
    @test_throws RedisError set("already", "subscribed"; client=subscriber)
    @test_throws RedisError ssubscribe("alreadsubscribed"; client=subscriber) do msg end
    
    sunsubscribe(channels[1]; client=subscriber)
    
    wait_until_shard_unsubscribed(subscriber, channels[1])
    @test subscriber.is_subscribed == true
    @test subscriber.ssubscriptions == Set{String}([channels[2], channels[3]])
    
    @test spublish(channels[1], "not subscribed anymore"; client=publisher) == 0
    
    @test length(messages) == 2
    
    sunsubscribe(; client=subscriber) # unsubscribe from everything
    
    wait_until_shard_unsubscribed(subscriber)
    @test subscriber.is_subscribed == false
    @test isempty(subscriber.ssubscriptions)
    
    stop_fn(msg) = msg[end] == "close subscription"
    
    @async ssubscribe(channels...; stop_fn=stop_fn, client=subscriber) do msg end
    
    wait_until_subscribed(subscriber)
    @test subscriber.is_subscribed == true
    @test subscriber.ssubscriptions == Set{String}(channels)
    
    @test spublish(channels[1], "close subscription"; client=publisher) == 1
    
    wait_until_shard_unsubscribed(subscriber)
    @test subscriber.is_subscribed == false
    @test isempty(subscriber.ssubscriptions)

    task = @async ssubscribe(channels...; client=subscriber) do msg
        push!(messages, msg)
    end
    
    wait_until_subscribed(subscriber)
    disconnect!(subscriber)  # force close
    
    @test subscriber.is_subscribed == false
    @test isempty(subscriber.ssubscriptions)
    @test istaskdone(task)
    try
        fetch(task)
    catch err
        @test err isa TaskFailedException
        @test err.task.result isa Base.IOError
        @test err.task.result.code == Base.UV_ECONNABORTED
    end
end

flushall()