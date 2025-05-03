using System;
using System.Linq;
using K4os.Orleans.Streaming.Redis.Storage;
using Newtonsoft.Json;
using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace K4os.Orleans.Streaming.Redis.Streams;

[Serializable]
[GenerateSerializer]
[Alias("K4os.Orleans.Streaming.Redis.Streams.RedisBatchContainer.v1")]
internal class RedisBatchContainer: IBatchContainer
{
    private static readonly EventSequenceTokenV2 EmptySequenceToken = new(0);

    [Id(0), JsonProperty("sequenceToken")]
    private EventSequenceTokenV2? _sequenceToken;

    [Id(1), JsonProperty("events")]
    private readonly List<object> _events;

    [Id(2), JsonProperty("requestContext")]
    private readonly Dictionary<string, object>? _requestContext;

    [Id(3), JsonProperty("streamId")]
    private StreamId _streamId;

    [NonSerialized]
    private RedisReceipt? _receipt;

    public StreamId StreamId => _streamId;
    public StreamSequenceToken? SequenceToken => _sequenceToken;
    public RedisReceipt? Receipt => _receipt;

    [JsonConstructor]
    private RedisBatchContainer(
        StreamId streamId,
        List<object> events,
        Dictionary<string, object> requestContext,
        EventSequenceTokenV2 sequenceToken):
        this(streamId, events, requestContext)
    {
        _sequenceToken = sequenceToken;
    }

    private RedisBatchContainer(
        StreamId streamId,
        List<object> events,
        Dictionary<string, object> requestContext)
    {
        ArgumentNullException.ThrowIfNull(events);
        _streamId = streamId;
        _events = events;
        _requestContext = requestContext;
    }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>() =>
        _events.OfType<T>().Select(GetEventTuple);

    private Tuple<T, StreamSequenceToken> GetEventTuple<T>(T @event, int index) =>
        new(@event, (_sequenceToken ?? EmptySequenceToken).CreateSequenceTokenForEvent(index));

    internal static RedisMessage ToRedisMessage<T>(
        Serializer<RedisBatchContainer> serializer,
        StreamId streamId,
        IEnumerable<T> events,
        Dictionary<string, object> requestContext)
    {
        var batch = new RedisBatchContainer(streamId, events.Cast<object>().ToList(), requestContext);
        var bytes = serializer.SerializeToArray(batch);
        return new RedisMessage(null, bytes);
    }

    internal static RedisBatchContainer FromRedisMessage(
        Serializer<RedisBatchContainer> serializer, RedisReceipt receipt, long sequenceId)
    {
        var batch = serializer.Deserialize(receipt.Message.Body);
        batch.OnAfterDeserialize(receipt, sequenceId);
        return batch;
    }

    private void OnAfterDeserialize(RedisReceipt receipt, long sequenceId)
    {
        _receipt = receipt;
        _sequenceToken = new EventSequenceTokenV2(sequenceId);
    }

    public bool ImportRequestContext()
    {
        if (_requestContext is null) return false;

        RequestContextExtensions.Import(_requestContext);
        return true;
    }

    public override string ToString() =>
        $"[{nameof(RedisBatchContainer)}:Stream={StreamId},#Items={_events.Count}]";
}
