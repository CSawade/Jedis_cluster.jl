using Jedis
using Test
using Dates

# Failed - normal - Quit Ping (1)
# Failed - cluster - 4 failed, 3 errored - Multi/Exec, HASH, LIST, QUIT
@testset "Commands" begin include("test_commands.jl") end

# Passing - normal
# Passing  - cluster 
# TODO: Add sharded subs
@testset "Pub/Sub" begin include("test_pubsub.jl") end

# Passing - normal
# Failed  - cluster - Multi/Exec (3)
@testset "Pipeline" begin include("test_pipeline.jl") end

# @testset "SSL/TLS" begin include("test_ssl.jl") end

# passing - normal
# passing  - cluster
@testset "Redis Locks" begin include("test_lock.jl") end