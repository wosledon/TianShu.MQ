using System;
using System.Threading;
using System.Threading.Tasks;

namespace TianShu.MQ.Core;

/// <summary>
/// ACK 确认接口（手动提交 offset）
/// </summary>
public interface IAck
{
    /// <summary>确认消息已处理，提交 offset</summary>
    Task CommitAsync();

    /// <summary>拒绝消息（不提交 offset，下次会重试）</summary>
    Task RejectAsync();
}

/// <summary>
/// 消息消费者接口
/// </summary>
public interface IMessageConsumer : IAsyncDisposable
{
    /// <summary>
    /// 订阅主题（Push 模式，回调消费，自动 ACK）
    /// </summary>
    Task SubscribeAsync(
        string topic,
        string groupId,
        Func<Message, Task<ConsumeResult>> handler,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 订阅主题（Push 模式，回调消费，手动 ACK）
    /// </summary>
    Task SubscribeAsync(
        string topic,
        string groupId,
        Func<Message, IAck, Task> handler,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 拉取消息（Pull 模式）
    /// </summary>
    Task<Message[]> PullAsync(
        string topic,
        string groupId,
        int maxBatchSize = 100,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 停止消费
    /// </summary>
    Task UnsubscribeAsync(string topic, string groupId, CancellationToken cancellationToken = default);
}
