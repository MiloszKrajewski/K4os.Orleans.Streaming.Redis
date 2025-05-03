using Microsoft.Extensions.Primitives;

namespace K4os.Orleans.Streaming.Redis.Storage;

public record RedisMessage(IDictionary<string, StringValues>? Headers, byte[]? Body);
