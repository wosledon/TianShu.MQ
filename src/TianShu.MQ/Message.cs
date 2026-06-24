using System;
using System.Collections.Generic;

namespace TianShu.MQ.Core;

/// <summary>
/// 消息实体
/// </summary>
public sealed class Message
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
    public DateTime? ScheduledEnqueueTime { get; init; }

    /// <summary>所属主题</summary>
    public string Topic { get; internal set; } = string.Empty;

    /// <summary>所属分区</summary>
    public int Partition { get; internal set; }
}
