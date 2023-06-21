set_global_client(retry_when_closed=false)

@testset "AUTH SELECT PING" begin
    @test_throws RedisError auth("")
    @test select(0) == "OK"
    @test ping() == "PONG"
end

@testset "GET SET" begin
    @test set("key", "value") == "OK"
    @test setnx("key", "value") == 0
    @test get("key") == "value"
    @test Jedis.keys("k*") == ["key"]
    @test del("key") == 1
    @test setnx("key", "value") == 1
    @test set("key", "vαlue") == "OK" # Has non-ascii
    @test get("key") == ("vαlue")# Has non-ascii
    @test flushdb() == "OK" && isnothing(get("key"))
end

@testset "EXPIRE" begin
    @test setex("key", 10, "value") == "OK"
    @test ttl("key") > 0
    @test del("key") == 1 && isnothing(get("key"))
    @test set("key", "value") == "OK"
    @test ttl("key") == -1
    @test expire("key", 10) == 1
    @test ttl("key") > 0
    @test flushdb() == "OK" && isnothing(get("key"))
end

if Jedis.GLOBAL_CLIENT[].cluster == false  # Redis cluster mode does not support MULTI EXEC
    @testset "MULTI EXEC" begin
        @test multi() == "OK"
        @test set("key", "value") == "QUEUED"
        @test get("key") == "QUEUED"
        @test get("key") == "QUEUED"
        @test exec() == ["OK", "value", "value"]
        @test ["OK", "value", "value"] == multi_exec() do 
            set("key", "value")
            get("key")
            get("key")
        end
        @test flushall() == "OK" && isnothing(get("key"))
    end
end

@testset "HASH" begin
    @test hset("key", "f1", 1, "f2", 2, "f3", 3) == 3
    @test hget("key", "f2") == "2"
    @test hgetall("key") == ["f1", "1", "f2", "2", "f3", "3"]
    @test hmget("key", "f1", "f2", "doesnotexist") == ["1", "2", nothing]
    @test hdel("key", "f1") == 1
    @test isnothing(hget("key", "f1"))
    @test flushall() == "OK" && isnothing(hget("key","f3"))
end

@testset "LIST" begin
    if Jedis.GLOBAL_CLIENT[].cluster == false
        keys = ["mylist", "otherlist"]  # Redis non-cluster mode, keys can be across slots
    else
        keys = ["{mylist}:1", "{mylist}:2"]  # Redis cluster mode, keys must be in the same slot
    end

    @test lpush(keys[1], 3, 2, 1) == 3
    @test rpush(keys[1], 4, 5, 6) == 6
    @test llen(keys[1]) == 6
    @test lrange(keys[1], 0, -1) == ["1", "2", "3", "4", "5", "6"]
    @test lpop(keys[1]) == "1"
    @test rpop(keys[1]) == "6"
    @test lrange(keys[1], 0, -1) == ["2", "3", "4", "5"]
    flushall()
    @test begin
        task = @async blpop(keys[1], keys[2]) == [keys[2], "1"]
        other = Client()
        lpush(keys[2], 1; client=other)
        disconnect!(other)
        fetch(task)
    end
    @test begin
        task = @async brpop(keys[1], keys[2]) == [keys[1], "6"]
        other = Client()
        lpush(keys[1], 6; client=other)
        disconnect!(other)
        fetch(task)
    end
    flushall()
end

@testset "ZFUNC" begin
    @test zadd("testscore", 1, "test1") == 1
    @test zadd("testscore", 2, "test2") == 1
    @test zrange("testscore", 0, -1) == ["test1", "test2"]
    @test zrange("testscore", 0, 1) == ["test1", "test2"]
    @test zrange("testscore", 1, 1) == ["test2"]
    @test zrangebyscore("testscore", 1, 1) == ["test1"]
    @test zrem("testscore", "test1") == 1
    @test zadd("testscore", 1, "test1", 3, "test3") == 2
    @test zrem("testscore", "test1", "test3") == 2
    @test zrange("testscore", 0, -1) == ["test2"]
    flushall()
end

@testset "QUIT" begin
    @test quit() == "OK"
    @test isclosed(get_global_client())
    @test_throws Base.IOError ping()
end

set_global_client(retry_when_closed=true)

@testset "RETRY" begin
    @test quit() == "OK"
    @test ping() == "PONG"
    @test !isclosed(get_global_client())
end