using System;
using System.Linq;
using Microsoft.Extensions.Primitives;
using StackExchange.Redis;

namespace K4os.Orleans.Streaming.Redis.Storage;

public class RedisConsumer
{
    public const int MaxBatchSize = 128;

    public static readonly TimeSpan ReclaimInterval = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan TrimInterval = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan TrimJitter = TimeSpan.FromSeconds(3);
    public static readonly TimeSpan ClaimTimeout = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan ConsumerTimeout = TimeSpan.FromDays(1);

    private static readonly LuaScript PurgeIdleConsumers = LuaScript.Prepare(
        """
        local function array_to_map(array)
        	local map = {}
        	for j = 1,#array,2 do map[array[j]] = array[j + 1] end
        	return map
        end

        local result = 0

        local consumers = redis.call("XINFO", "CONSUMERS", @stream, @group)
        local timeout = tonumber(@idle)

        for c = 1,#consumers do
        	local consumer = array_to_map(consumers[c])
        	if consumer.pending <= 0 and consumer.idle >= timeout then
        		redis.call("XGROUP", "DELCONSUMER", @stream, @group, consumer.name)
        		result = result + 1
        	end
        end

        return result
        """);

    private static readonly LuaScript TrimConsumedMessages = LuaScript.Prepare(
        """
        local function array_to_map(array)
        	local map = {}
        	for j = 1,#array,2 do map[array[j]] = array[j + 1] end
        	return map
        end

        local function extract_id(text)
        	local id = text:match("^(%d+)")
        	return id and tonumber(id) or nil
        end

        local function min_id(a, b)
        	if a == nil then return b else return math.min(a, b) end
        end

        local delivered = nil

        local groups = redis.call("XINFO", "GROUPS", @stream)
        for g = 1,#groups do
        	local group = array_to_map(groups[g])
        	local gid = extract_id(group["last-delivered-id"])
        	delivered = min_id(delivered, gid)
        	local pending = redis.call("XPENDING", @stream, group.name)
        	local pcount = tonumber(pending[1])
        	if pcount > 0 then
        		local pid = extract_id(pending[2])
        		delivered = min_id(delivered, pid)
        	end
        end

        if delivered then
        	redis.call("XTRIM", @stream, "MINID", delivered)
        	return delivered .. "-0"
        end

        return nil
        """);

    private readonly IDatabase _redis;
    private readonly RedisKey _stream;
    private readonly RedisValue _group;
    private readonly RedisValue _consumer;

    private bool _initialized;
    private DateTime _nextClaim;
    private DateTime _nextTrim;
    private int _lastClaimCount;
    private readonly TimeProvider _timeProvider;

    public RedisConsumer(
        IDatabase redis,
        string stream, string group,
        TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _redis = redis;
        _stream = (RedisKey)stream;
        _group = group;
        _consumer = Guid.NewGuid().ToShortGuid();
        _nextClaim = DateTime.MinValue;
        _nextTrim = DateTime.MinValue;
        _initialized = false;
    }

    private DateTime Now => _timeProvider.GetUtcNow().DateTime;

    public async Task<RedisReceipt[]> Dequeue(int count = 1)
    {
        if (!_initialized)
        {
            await InitConsumer();
            _initialized = true;
        }

        if (Now >= _nextTrim)
        {
            await TrimStream();
            _nextTrim = Now.Add(TrimInterval).Jitter(TrimJitter);
        }

        count = Math.Clamp(count, 1, MaxBatchSize);

        if (_lastClaimCount > 0 || Now >= _nextClaim)
        {
            var result = await Reclaim(count);
            var claimed = result.ClaimedEntries;
            _nextClaim = Now.Add(ReclaimInterval);
            if ((_lastClaimCount = claimed.Length) > 0)
                return ToRedisReceipts(result.ClaimedEntries);
        }

        var messages = await ReadGroup(count);
        return ToRedisReceipts(messages);
    }

    public Task Acknowledge(RedisReceipt[] messages) =>
        Acknowledge(messages.Transform(static m => (RedisValue)m.Id));

    public Task Acknowledge(string[] messages) =>
        Acknowledge(messages.Transform(static m => (RedisValue)m));

    private Task Acknowledge(RedisValue[] messages) =>
        messages.Length > 0
            ? _redis.StreamAcknowledgeAsync(_stream, _group, messages)
            : Task.CompletedTask;

    private async Task InitConsumer()
    {
        try
        {
            await _redis.StreamCreateConsumerGroupAsync(_stream, _group);
        }
        catch (RedisServerException rse) when (rse.Message.StartsWith("BUSYGROUP"))
        {
            // ignore, this means the group already exists
        }
    }

    private async Task TrimStream()
    {
        var timeout = (long)ConsumerTimeout.TotalMilliseconds;
        await _redis.ScriptEvaluateAsync(PurgeIdleConsumers, new { stream = _stream, group = _group, idle = timeout });
        await _redis.ScriptEvaluateAsync(TrimConsumedMessages, new { stream = _stream });
    }

    private Task<StreamAutoClaimResult> Reclaim(int count) =>
        _redis.StreamAutoClaimAsync(_stream, _group, _consumer, (int)ClaimTimeout.TotalMilliseconds, "0-0", count);

    private Task<StreamEntry[]> ReadGroup(int count) =>
        _redis.StreamReadGroupAsync(_stream, _group, _consumer, null, count);

    private static RedisReceipt[] ToRedisReceipts(StreamEntry[] entries) =>
        entries.Transform(static e => ToRedisReceipt(e));

    private static RedisReceipt ToRedisReceipt(StreamEntry entry) =>
        ToRedisReceipt(entry.Id, entry.Values);

    private static RedisReceipt ToRedisReceipt(RedisValue id, NameValueEntry[] values)
    {
        Dictionary<string, StringValues>? headers = null;
        byte[]? body = null;

        foreach (var kv in values)
        {
            var name = (string)kv.Name!;
            var value = kv.Value;

            if (name == RedisStream.BodyField)
            {
                body = (byte[]?)value;
            }
            else
            {
                headers ??= new Dictionary<string, StringValues>();
                headers[name] = (StringValues)(string)value!;
            }
        }

        return new RedisReceipt(id!, new RedisMessage(headers, body));
    }
}
