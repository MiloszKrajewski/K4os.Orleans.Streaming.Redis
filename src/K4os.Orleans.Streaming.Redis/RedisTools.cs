using Orleans.Configuration;
using Orleans.Streams;
using StackExchange.Redis;

namespace K4os.Orleans.Streaming.Redis;

internal class RedisTools
{
    public static string StreamName(string serviceId, QueueId queueId) =>
        $"{serviceId}/stream/{queueId}";

    public static async Task<IConnectionMultiplexer> CreateMultiplexer(RedisStreamingOptions options) =>
        await ConnectionMultiplexer.ConnectAsync(options.ConnectionOptions);

}
