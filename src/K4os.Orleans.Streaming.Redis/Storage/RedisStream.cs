using System;
using System.Linq;
using Microsoft.Extensions.Primitives;
using StackExchange.Redis;

namespace K4os.Orleans.Streaming.Redis.Storage;

public class RedisStream
{
    internal const string BodyField = "$";

	private readonly IDatabase _redis;
    private readonly RedisKey _stream;

	public RedisStream(IDatabase redis, string stream)
	{
		_redis = redis;
		_stream = (RedisKey)stream;
	}

    public Task RemoveStream() => _redis.KeyDeleteAsync(_stream);

    public RedisConsumer CreateConsumer(string group) =>
        new(_redis, _stream!, group);

    public Task Enqueue(RedisMessage message) =>
        _redis.StreamAddAsync(_stream, ToNameValueEntries(message));

    public Task Enqueue(IBatch batch, RedisMessage message) =>
        batch.StreamAddAsync(_stream, ToNameValueEntries(message));

    public Task Enqueue(RedisMessage[] messages)
    {
        var batch = _redis.CreateBatch();
        var results = messages.Transform(m => Enqueue(batch, m));
        batch.Execute();
        return Task.WhenAll(results);
    }

    private static NameValueEntry[] ToNameValueEntries(RedisMessage message) =>
        ToNameValueEntries(message.Headers, message.Body);

    private static NameValueEntry[] ToNameValueEntries(IDictionary<string, StringValues>? headers, byte[]? body)
    {
        var count = (body is not null ? 1 : 0) + (headers?.Count ?? 0);
        var entries = new NameValueEntry[count];
        var index = 0;

        if (body is not null)
        {
            entries[index++] = new NameValueEntry(BodyField, body);
        }

        if (headers?.Count > 0)
        {
            foreach (var (k, v) in headers)
                entries[index++] = new NameValueEntry(k, v.ToString());
        }

        return entries;
    }
}
