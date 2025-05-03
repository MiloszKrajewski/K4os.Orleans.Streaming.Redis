using System;
using K4os.Orleans.Streaming.Redis.Streams;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

// ReSharper disable once CheckNamespace
namespace Orleans.Configuration;

public class SiloRedisStreamConfigurator : SiloPersistentStreamConfigurator
{
    public SiloRedisStreamConfigurator(string name, Action<Action<IServiceCollection>> configureServicesDelegate) :
        base(name, configureServicesDelegate, RedisAdapterFactory.Create)
    {
        ConfigureDelegate(services => services
            .ConfigureNamedOptionForLogging<RedisStreamingOptions>(name)
            .ConfigureNamedOptionForLogging<SimpleQueueCacheOptions>(name)
            .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name));
    }

    public SiloRedisStreamConfigurator ConfigureRedis(Action<OptionsBuilder<RedisStreamingOptions>> configureOptions)
    {
        this.Configure(configureOptions);
        return this;
    }

    public SiloRedisStreamConfigurator ConfigureCache(int cacheSize = SimpleQueueCacheOptions.DEFAULT_CACHE_SIZE)
    {
        this.Configure<SimpleQueueCacheOptions>(ob => ob
            .Configure(options => options.CacheSize = cacheSize));
        return this;
    }

    public SiloRedisStreamConfigurator ConfigurePartitioning(
        int numOfPartitions = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        this.Configure<HashRingStreamQueueMapperOptions>(ob => ob
            .Configure(options => options.TotalQueueCount = numOfPartitions));
        return this;
    }
}
