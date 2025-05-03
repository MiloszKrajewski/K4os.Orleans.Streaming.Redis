using System;
using Orleans.Configuration;

// ReSharper disable once CheckNamespace
namespace Orleans.Hosting;

public static class SiloBuilderExtensions
{
    /// <summary>
    /// Configure silo to use SQS persistent streams.
    /// </summary>
    public static ISiloBuilder AddRedisStreams(
        this ISiloBuilder builder, string name, Action<RedisStreamingOptions> configure)
    {
        builder.AddRedisStreams(name, b => b.ConfigureRedis(ob => ob.Configure(configure)));
        return builder;
    }

    /// <summary>
    /// Configure silo to use SQS persistent streams.
    /// </summary>
    public static ISiloBuilder AddRedisStreams(
        this ISiloBuilder builder, string name, Action<SiloRedisStreamConfigurator> configure)
    {
        var configurator = new SiloRedisStreamConfigurator(name, cb => builder.ConfigureServices(cb));
        configure.Invoke(configurator);
        return builder;
    }
}