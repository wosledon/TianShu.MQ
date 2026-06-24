using System;
using System.Collections.Generic;
using TianShu.MQ.Serialization;

namespace TianShu.MQ.Core;

/// <summary>
/// 消息实体
/// </summary>
public class Message
{
    /// <summary>消息唯一 ID</summary>
    public string MessageId { get; init; } = Guid.NewGuid().ToString("N");

    /// <summary>消息体（序列化后的字节数组）</summary>
    public byte[] Body { get; init; } = Array.Empty<byte>();

    /// <summary>分区键（决定消息进入哪个分区）</summary>
    public string PartitionKey { get; init; } = string.Empty;

    /// <summary>消息在分区内的偏移量</summary>
    public long Offset { get; internal set; }

    /// <summary>消息头（自定义元数据）</summary>
    public Dictionary<string, string> Headers { get; init; } = new();

    /// <summary>消息创建时间戳</summary>
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    /// <summary>延迟投递时间（到期后消息才可见）</summary>
    public DateTime? ScheduledEnqueueTime { get; set; }

    /// <summary>所属主题</summary>
    public string Topic { get; internal set; } = string.Empty;

    /// <summary>所属分区</summary>
    public int Partition { get; internal set; }
}

/// <summary>
/// 泛型消息实体（类型化的消息体，自动序列化/反序列化）
/// </summary>
public sealed class Message<T> : Message
{
    /// <summary>
    /// 类型化的消息体
    /// </summary>
    public new T Body { get; init; } = default!;

    /// <summary>
    /// 从 Message 转换（反序列化 body）
    /// </summary>
    public static Message<T> From(Message msg, IMessageSerializer serializer)
    {
        return new Message<T>
        {
            MessageId = msg.MessageId,
            Body = serializer.Deserialize<T>(msg.Body),
            PartitionKey = msg.PartitionKey,
            Offset = msg.Offset,
            Headers = msg.Headers,
            Timestamp = msg.Timestamp,
            ScheduledEnqueueTime = msg.ScheduledEnqueueTime,
            Topic = msg.Topic,
            Partition = msg.Partition
        };
    }

    /// <summary>
    /// 转换为无类型 Message（序列化 body）
    /// </summary>
    public Message ToUntyped(IMessageSerializer serializer)
    {
        return new Message
        {
            MessageId = MessageId,
            Body = serializer.Serialize(Body),
            PartitionKey = PartitionKey,
            Offset = Offset,
            Headers = Headers,
            Timestamp = Timestamp,
            ScheduledEnqueueTime = ScheduledEnqueueTime,
            Topic = Topic,
            Partition = Partition
        };
    }
}
