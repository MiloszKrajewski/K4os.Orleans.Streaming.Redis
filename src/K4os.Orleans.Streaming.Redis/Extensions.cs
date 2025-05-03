using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;

namespace K4os.Orleans.Streaming.Redis;

internal static class Extensions
{
    public static TOutput[] Transform<TInput, TOutput>(
        this Span<TInput> span, Func<TInput, TOutput> transform)
    {
        var length = span.Length;
        var result = new TOutput[length];
        for (var i = 0; i < length; i++)
            result[i] = transform(span[i]);
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static TOutput[] Transform<TInput, TOutput>(
        this TInput[] collection, Func<TInput, TOutput> transform) =>
        Transform(collection.AsSpan(), transform);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string? NullIfBlank(this string? text) =>
        string.IsNullOrWhiteSpace(text) ? null : text;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T ThrowIfNull<T>(
        [NotNull] this T? subject,
        [CallerArgumentExpression(nameof(subject))] string? expression = null) =>
        subject ?? ThrowArgumentNull<T>(expression);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [DoesNotReturn]
    private static T ThrowArgumentNull<T>(string? expression) =>
        throw new ArgumentNullException(expression);

    // this is suboptimal implementation, but it is used only when creating consumers
    public static string ToShortGuid(this Guid guid) =>
        Convert.ToBase64String(guid.ToByteArray()).Replace("/", "_").Replace("+", "-").TrimEnd('=');

    public static DateTime Jitter(this DateTime dateTime, TimeSpan jitter) =>
        dateTime.Add(TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * jitter.TotalMilliseconds));
}
