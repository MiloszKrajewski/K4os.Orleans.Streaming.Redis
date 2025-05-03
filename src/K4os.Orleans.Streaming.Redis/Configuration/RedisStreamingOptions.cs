using System;
using StackExchange.Redis;

// ReSharper disable once CheckNamespace
namespace Orleans.Configuration;

public class RedisStreamingOptions
{
    [Redact]
    public ConfigurationOptions ConnectionOptions { get; set; } = new();

    public int? DatabaseId { get; set; }

    public Func<RedisStreamingOptions, Task<IConnectionMultiplexer>>? MultiplexerFactory { get; set; }
}
