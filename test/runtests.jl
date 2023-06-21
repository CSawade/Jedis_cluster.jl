using Jedis
using Test
using Dates

# Passing - normal
# Passing - cluster
@testset "Commands" begin include("test_commands.jl") end

# Passing - normal
# Passing  - cluster 
@testset "Pub/Sub" begin include("test_pubsub.jl") end

# Passing - normal
# Passing  - cluster
@testset "Pipeline" begin include("test_pipeline.jl") end

# @testset "SSL/TLS" begin include("test_ssl.jl") end

# passing - normal
# passing  - cluster
@testset "Redis Locks" begin include("test_lock.jl") end