using System;
using System.Linq;
using K4os.Orleans.Streaming.Redis.Storage;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streams;

namespace K4os.Orleans.Streaming.Redis.Streams;

/// <summary>
/// Receives batches of messages from a single partition of a message queue.
/// </summary>
internal partial class RedisAdapterReceiver: IQueueAdapterReceiver
{
    private const int MaxNumberOfMessageToPeek = 10;

    private readonly QueueId _queueId;

    private long _lastReadMessage;
    private Task? _outstandingTask;
    private readonly ILogger _logger;
    private readonly Serializer<RedisBatchContainer> _serializer;

    private RedisConsumer? _consumer;

    public QueueId Id => _queueId;

    public static IQueueAdapterReceiver Create(
        ILoggerFactory loggerFactory,
        Serializer<RedisBatchContainer> serializer,
        RedisConsumer consumer,
        QueueId queueId) =>
        new RedisAdapterReceiver(loggerFactory, serializer, consumer, queueId);

    private RedisAdapterReceiver(
        ILoggerFactory loggerFactory,
        Serializer<RedisBatchContainer> serializer,
        RedisConsumer redisConsumer,
        QueueId queueId)
    {
        _queueId = queueId;
        _consumer = redisConsumer;
        _logger = loggerFactory.CreateLogger<RedisAdapterReceiver>();
        _serializer = serializer;
    }

    public async Task Shutdown(TimeSpan timeout)
    {
        try
        {
            // await the last storage operation, so after we shutdown and stop this receiver we
            // don't get async operation completions from pending storage operations.
            await (_outstandingTask ?? Task.CompletedTask);
        }
        finally
        {
            // remember that we shut down so we never try to read from the queue again.
            _consumer = null;
        }
    }

    public Task Initialize(TimeSpan timeout) => Task.CompletedTask;

    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
    {
        try
        {
            var consumer =
                _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
            if (consumer is null) return new List<IBatchContainer>();

            var count = maxCount < 0
                ? MaxNumberOfMessageToPeek
                : Math.Min(maxCount, MaxNumberOfMessageToPeek);

            var dequeueTask = consumer.Dequeue(count);
            _outstandingTask = dequeueTask;
            var messages = await dequeueTask;

            return messages.Select(UnpackRedisMessage).ToList();
        }
        finally
        {
            _outstandingTask = null;
        }
    }

    private IBatchContainer UnpackRedisMessage(RedisReceipt m) =>
        RedisBatchContainer.FromRedisMessage(_serializer, m, _lastReadMessage++);

    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        try
        {
            // var queue = _queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
            var consumer = _consumer;
            if (messages.Count == 0 || consumer == null) return;

            var receipts = messages
                .Cast<RedisBatchContainer>()
                .Select(b => b.Receipt).OfType<RedisReceipt>()
                .ToArray();
            _outstandingTask = consumer.Acknowledge(receipts);

            try
            {
                await _outstandingTask;
            }
            catch (Exception exc)
            {
                LogWarningDeleteMessageException(_logger, exc, Id);
            }
        }
        finally
        {
            _outstandingTask = null;
        }
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Exception upon DeleteMessage on queue {Id}. Ignoring."
    )]
    private static partial void LogWarningDeleteMessageException(ILogger logger, Exception exception, QueueId id);
}
