using System;
using System.Threading;
using System.Threading.Tasks;

namespace TianShu.MQ.Core;

/// <summary>
/// 消息消费者接口
/// </summary>
public interface IMessageConsumer : IAsyncDisposable
{
    /// <summary>
    /// 订阅主题（Push 模式，回调消费）
    /// </summary>
    Task SubscribeAsync(
        string topic,
        string groupId,
        Func<Message, Task<ConsumeResult>> handler,
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
