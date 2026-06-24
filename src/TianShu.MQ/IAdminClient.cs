using System;
using System.Threading;
using System.Threading.Tasks;

namespace TianShu.MQ.Core;

/// <summary>
/// 管理客户端接口
/// </summary>
public interface IAdminClient
{
    /// <summary>
    /// 创建主题
    /// </summary>
    Task CreateTopicAsync(TopicOptions options, CancellationToken cancellationToken = default);

    /// <summary>
    /// 删除主题
    /// </summary>
    Task DeleteTopicAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取主题统计信息
    /// </summary>
    Task<TopicStats> GetTopicStatsAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// 获取消费者组进度
    /// </summary>
    Task<ConsumerGroupProgress> GetConsumerGroupProgressAsync(string topic, string groupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// 重置消费者偏移量
    /// </summary>
    Task ResetOffsetAsync(string topic, string groupId, OffsetReset strategy, DateTime? timestamp = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// 列出所有主题
    /// </summary>
    string[] ListTopics();
}
