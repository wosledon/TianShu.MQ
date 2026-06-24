using System;
using System.Threading;
using System.Threading.Tasks;

namespace TianShu.MQ.Core;

/// <summary>
/// 消息生产者接口
/// </summary>
public interface IMessageProducer : IAsyncDisposable
{
    /// <summary>
    /// 发布消息到指定主题
    /// </summary>
    Task PublishAsync(string topic, Message message, CancellationToken cancellationToken = default);

    /// <summary>
    /// 发布消息到指定主题（指定分区键+消息体）
    /// </summary>
    Task PublishAsync<T>(string topic, string? partitionKey, T body, CancellationToken cancellationToken = default);

    /// <summary>
    /// 发布消息到指定主题（指定分区键+消息体+延迟时间）
    /// </summary>
    Task PublishAsync<T>(string topic, string? partitionKey, T body, DateTime scheduledTime, CancellationToken cancellationToken = default);

    /// <summary>
    /// 批量发布消息
    /// </summary>
    Task PublishBatchAsync(string topic, Message[] messages, CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取主题当前的写入位置（各分区最新 Offset）
    /// </summary>
    long[] GetTopicOffsets(string topic);
}
