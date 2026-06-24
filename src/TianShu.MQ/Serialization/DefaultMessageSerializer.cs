using System.Text.Json;

namespace TianShu.MQ.Serialization;

/// <summary>
/// 默认 JSON 序列化器实现
/// </summary>
public sealed class DefaultMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public byte[] Serialize<T>(T obj)
    {
        return JsonSerializer.SerializeToUtf8Bytes(obj, Options);
    }

    public T Deserialize<T>(byte[] data)
    {
        return JsonSerializer.Deserialize<T>(data, Options)!;
    }
}
