using System;
using System.Threading;
using System.Threading.Tasks;
using TianShu.MQ.Core;

namespace TianShu.MQ.Storage;

/// <summary>
/// 存储引擎接口
/// </summary>
public interface IStorageEngine : IAsyncDisposable
{
    /// <summary>写入消息，返回分配的 Offset</summary>
    Task<long> AppendAsync(string topic, int partition, Message message, CancellationToken cancellationToken = default);

    /// <summary>批量写入消息</summary>
    Task<long[]> AppendBatchAsync(string topic, int partition, Message[] messages, CancellationToken cancellationToken = default);

    /// <summary>读取指定 offset 开始的消息</summary>
    Task<Message[]> ReadAsync(string topic, int partition, long offset, int count, CancellationToken cancellationToken = default);

    /// <summary>获取指定分区当前最新 offset（下一条消息的 offset）</summary>
    long GetLatestOffset(string topic, int partition);

    /// <summary>获取指定分区最早 offset</summary>
    long GetEarliestOffset(string topic, int partition);

    /// <summary>初始化存储（创建 Topic/Partition）</summary>
    Task InitializeAsync(string topic, int partitions, CancellationToken cancellationToken = default);

    /// <summary>删除 Topic 所有数据</summary>
    Task DeleteAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>等待新消息到达（长轮询）</summary>
    Task WaitForMessagesAsync(string topic, int partition, long offset, TimeSpan timeout, CancellationToken cancellationToken = default);
}
