using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;
using RedisStream = K4os.Orleans.Streaming.Redis.Storage.RedisStream;

namespace K4os.Orleans.Streaming.Redis.Streams;

internal class RedisAdapter: IQueueAdapter
{
    private readonly ConcurrentDictionary<QueueId, RedisStream> _queues = new();

    private readonly string _serviceId;
    private readonly string _providerName;
    private readonly IDatabase _redisDatabase;
    private readonly Serializer<RedisBatchContainer> _serializer;
    private readonly IConsistentRingStreamQueueMapper _streamQueueMapper;
    private readonly ILoggerFactory _loggerFactory;

    public string Name => _providerName;
    public bool IsRewindable => false;

    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public RedisAdapter(
        ILoggerFactory loggerFactory,
        Serializer<RedisBatchContainer> serializer,
        IConsistentRingStreamQueueMapper streamQueueMapper,
        IDatabase redisDatabase,
        string serviceId,
        string providerName)
    {
        _serviceId = serviceId;
        _providerName = providerName;
        _loggerFactory = loggerFactory;
        _serializer = serializer;
        _streamQueueMapper = streamQueueMapper;
        _redisDatabase = redisDatabase;
    }

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        var stream = GetOrCreateStream(queueId);
        var consumer = stream.CreateConsumer("orleans");
        return RedisAdapterReceiver.Create(_loggerFactory, _serializer, consumer, queueId);
    }

    public async Task QueueMessageBatchAsync<T>(
        StreamId streamId,
        IEnumerable<T> events,
        StreamSequenceToken? token,
        Dictionary<string, object> requestContext)
    {
        TokenMustBeNull(token);

        var queueId = _streamQueueMapper.GetQueueForStream(streamId);
        var stream = GetOrCreateStream(queueId);
        var message = RedisBatchContainer.ToRedisMessage(_serializer, streamId, events, requestContext);
        await stream.Enqueue(message);
    }

    private RedisStream GetOrCreateStream(QueueId queueId) =>
        _queues.GetOrAdd(queueId, CreateStream);

    private RedisStream CreateStream(QueueId queueId) =>
        new(_redisDatabase, RedisTools.StreamName(_serviceId, queueId));

    private static void TokenMustBeNull(StreamSequenceToken? token)
    {
        if (token is null) return;

        throw new ArgumentException(
            @"RedisStream stream provider currently does not support non-null StreamSequenceToken",
            nameof(token));
    }
}
