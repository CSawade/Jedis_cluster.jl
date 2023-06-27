module Jedis

export Client, Pipeline, RedisError, get_global_client, set_global_client, get_ssl_config, 
       disconnect!, reconnect!, add!, copy, wait_until_subscribed, wait_until_unsubscribed, 
       wait_until_channel_unsubscribed, wait_until_pattern_unsubscribed, wait_until_shard_unsubscribed,
       execute, auth, select, ping, flushdb, flushall, quit, set, setnx, get, del, exists, hexists, 
       hkeys, setex, expire, ttl, multi, exec, multi_exec, pipeline, hset, hget, hgetall, hmget, 
       hdel, rpush, lpush, lpos, lrem, lpop, rpop, blpop, brpop, llen, lrange, publish, spublish, 
       subscribe, unsubscribe, psubscribe, punsubscribe, ssubscribe, sunsubscribe, incr, incrby, 
       incrbyfloat, hincrby, hincrbyfloat, zincrby, zadd, zrange, zrangebyscore, zrem, acquire_lock, 
       release_lock, redis_lock, isredislocked, isclosed, set_client_instance, set_client

using Sockets
using MbedTLS
using UUIDs: uuid4
using CRC
import Base: copy, showerror, get, pipeline

include("exceptions.jl")
include("utilities.jl")
include("client.jl")
include("stream.jl")
include("pipeline.jl")
include("protocol.jl")
include("execute.jl")
include("commands.jl")
include("pubsub.jl")
include("lock.jl")



end # module