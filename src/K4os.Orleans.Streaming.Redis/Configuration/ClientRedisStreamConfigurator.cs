using System;
using K4os.Orleans.Streaming.Redis.Streams;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

// ReSharper disable once CheckNamespace
namespace Orleans.Configuration;

public class ClientRedisStreamConfigurator : ClusterClientPersistentStreamConfigurator
{
    public ClientRedisStreamConfigurator(string name, IClientBuilder builder) :
        base(name, builder, RedisAdapterFactory.Create)
    {
        builder.ConfigureServices(services => services
            .ConfigureNamedOptionForLogging<RedisStreamingOptions>(name)
            .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name));
    }

    public ClientRedisStreamConfigurator ConfigureRedis(
        Action<OptionsBuilder<RedisStreamingOptions>> configureOptions)
    {
        this.Configure(configureOptions);
        return this;
    }

    public ClientRedisStreamConfigurator ConfigurePartitioning(
        int numOfPartitions = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        this.Configure<HashRingStreamQueueMapperOptions>(ob => ob
            .Configure(options => options.TotalQueueCount = numOfPartitions));
        return this;
    }
}
