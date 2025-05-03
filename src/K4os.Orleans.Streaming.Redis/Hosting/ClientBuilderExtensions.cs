using System;
using Orleans.Configuration;

// ReSharper disable once CheckNamespace
namespace Orleans.Hosting;

public static class ClientBuilderExtensions
{
    /// <summary>
    /// Configure cluster client to use SQS persistent streams with default settings
    /// </summary>
    public static IClientBuilder AddRedisStreams(
        this IClientBuilder builder, string name, Action<RedisStreamingOptions> configureOptions)
    {
        builder.AddRedisStreams(name, b => b.ConfigureRedis(ob => ob.Configure(configureOptions)));
        return builder;
    }

    /// <summary>
    /// Configure cluster client to use SQS persistent streams.
    /// </summary>
    public static IClientBuilder AddRedisStreams(
        this IClientBuilder builder, string name, Action<ClientRedisStreamConfigurator>? configure)
    {
        var configurator = new ClientRedisStreamConfigurator(name, builder);
        configure?.Invoke(configurator);
        return builder;
    }
}
