using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Configuration.Overrides;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace K4os.Orleans.Streaming.Redis.Streams;

/// <summary> Factory class for Azure Queue based stream provider.</summary>
public class RedisAdapterFactory: IQueueAdapterFactory
{
    private static readonly Task<IStreamFailureHandler> NoOpStreamDeliveryFailureHandler =
        Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());

    private readonly string _providerName;
    private readonly RedisStreamingOptions _redisOptions;
    private readonly ClusterOptions _clusterOptions;
    private readonly Serializer<RedisBatchContainer> _serializer;
    private readonly ILoggerFactory _loggerFactory;
    private readonly HashRingBasedStreamQueueMapper _streamQueueMapper;
    private readonly IQueueAdapterCache _adapterCache;

    /// <summary>
    /// Application level failure handler override.
    /// </summary>
    protected Func<QueueId, Task<IStreamFailureHandler>>? StreamFailureHandlerFactory { private get; set; }

    public static RedisAdapterFactory Create(IServiceProvider services, string name)
    {
        var redisOptions = services.GetOptionsByName<RedisStreamingOptions>(name);
        var cacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(name);
        var queueMapperOptions = services.GetOptionsByName<HashRingStreamQueueMapperOptions>(name);
        var clusterOptions = services.GetProviderClusterOptions(name);
        return ActivatorUtilities.CreateInstance<RedisAdapterFactory>(
            services, name, redisOptions, queueMapperOptions, cacheOptions, clusterOptions);
    }

    public RedisAdapterFactory(
        string name,
        RedisStreamingOptions redisOptions,
        HashRingStreamQueueMapperOptions queueMapperOptions,
        SimpleQueueCacheOptions cacheOptions,
        IOptions<ClusterOptions> clusterOptions,
        Serializer serializer,
        ILoggerFactory loggerFactory)
    {
        _providerName = name;
        _redisOptions = redisOptions;
        _clusterOptions = clusterOptions.Value;
        _serializer = serializer.GetSerializer<RedisBatchContainer>();
        _loggerFactory = loggerFactory;
        _streamQueueMapper = new HashRingBasedStreamQueueMapper(queueMapperOptions, _providerName);
        _adapterCache = new SimpleQueueAdapterCache(cacheOptions, _providerName, _loggerFactory);
    }

    /// <summary>Creates the Azure Queue based adapter.</summary>
    public virtual async Task<IQueueAdapter> CreateAdapter()
    {
        var multiplexerFactory = _redisOptions.MultiplexerFactory ?? RedisTools.CreateMultiplexer;
        var multiplexer = await multiplexerFactory(_redisOptions);
        var redisDatabase = multiplexer.GetDatabase(_redisOptions.DatabaseId ?? -1);

        return new RedisAdapter(
            _loggerFactory,
            _serializer,
            _streamQueueMapper,
            redisDatabase,
            _clusterOptions.ServiceId,
            _providerName);
    }

    /// <summary>Creates the adapter cache.</summary>
    public IQueueAdapterCache GetQueueAdapterCache() => _adapterCache;

    /// <summary>Creates the factory stream queue mapper.</summary>
    public IStreamQueueMapper GetStreamQueueMapper() => _streamQueueMapper;

    /// <summary>
    /// Creates a delivery failure handler for the specified queue.
    /// </summary>
    /// <param name="queueId"></param>
    /// <returns></returns>
    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) =>
        StreamFailureHandlerFactory?.Invoke(queueId) ?? NoOpStreamDeliveryFailureHandler;
}
