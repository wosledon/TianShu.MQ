using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TianShu.MQ.Core;
using TianShu.MQ.Storage;

namespace TianShu.MQ.Persist;

/// <summary>
/// 基于本地文件的持久化存储引擎
/// 采用追加写日志 + 内存索引的结构
/// </summary>
public sealed class PersistStorageEngine : IStorageEngine
{
    private readonly string _basePath;
    private readonly ConcurrentDictionary<string, TopicData> _topics = new();
    private readonly TimeSpan _flushInterval;

    public PersistStorageEngine(string basePath, TimeSpan? flushInterval = null)
    {
        _basePath = basePath;
        _flushInterval = flushInterval ?? TimeSpan.FromMilliseconds(100);
        Directory.CreateDirectory(_basePath);
    }

    private sealed class PartitionData
    {
        public long EarliestOffset = 0;
        public long NextOffset;
        public readonly List<Message> Messages = new();
        public readonly Channel<long> WaitChannel;
        public readonly object WriteLock = new();

        public PartitionData()
        {
            WaitChannel = Channel.CreateBounded<long>(new BoundedChannelOptions(1024)
            {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.DropOldest
            });
        }
    }

    private sealed class TopicData : IAsyncDisposable
    {
        public readonly PartitionData[] Partitions;
        public readonly string DataPath;
        private readonly Timer _flushTimer;
        private volatile bool _dirty;
        private readonly object _flushLock = new();

        public TopicData(int partitionCount, string dataPath, TimeSpan flushInterval)
        {
            Partitions = new PartitionData[partitionCount];
            for (int i = 0; i < partitionCount; i++)
            {
                Partitions[i] = new PartitionData();
            }
            DataPath = dataPath;
            _flushTimer = new Timer(_ => FlushIfNeeded(), null, flushInterval, flushInterval);
        }

        public void MarkDirty() => _dirty = true;

        private void FlushIfNeeded()
        {
            if (!_dirty) return;
            _dirty = false;
            FlushToFile();
        }

        public void FlushToFile()
        {
            lock (_flushLock)
            {
                try
                {
                    var allMessages = new List<Message>();
                    foreach (var part in Partitions)
                    {
                        lock (part.WriteLock)
                        {
                            allMessages.AddRange(part.Messages);
                        }
                    }
                    var json = JsonSerializer.Serialize(allMessages);
                    File.WriteAllText(DataPath, json);
                }
                catch
                {
                    // Log error but don't crash
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            _flushTimer.Dispose();
            FlushToFile(); // 最终刷盘
            return ValueTask.CompletedTask;
        }
    }

    public Task InitializeAsync(string topic, int partitions, CancellationToken cancellationToken = default)
    {
        var topicPath = Path.Combine(_basePath, topic);
        Directory.CreateDirectory(topicPath);

        var dataFile = Path.Combine(topicPath, "data.json");
        var topicData = new TopicData(partitions, dataFile, _flushInterval);

        // 加载已有数据
        if (File.Exists(dataFile))
        {
            try
            {
                var json = File.ReadAllText(dataFile);
                var messages = JsonSerializer.Deserialize<List<Message>>(json) ?? new();
                foreach (var msg in messages)
                {
                    if (msg.Partition >= 0 && msg.Partition < partitions)
                    {
                        var part = topicData.Partitions[msg.Partition];
                        // 恢复时重新分配 offset（因为 Offset 属性不会反序列化）
                        msg.Offset = part.NextOffset;
                        part.Messages.Add(msg);
                        part.NextOffset = msg.Offset + 1;
                    }
                }
            }
            catch
            {
                // 数据损坏，重新开始
            }
        }

        if (!_topics.TryAdd(topic, topicData))
            throw new InvalidOperationException($"Topic '{topic}' already exists.");

        return Task.CompletedTask;
    }

    public Task<long> AppendAsync(string topic, int partition, Message message, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryGetValue(topic, out var topicData))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var part = topicData.Partitions[partition];
        long offset;

        lock (part.WriteLock)
        {
            offset = part.NextOffset;
            message.Offset = offset;
            message.Topic = topic;
            message.Partition = partition;
            part.Messages.Add(message);
            part.NextOffset = offset + 1;
        }

        topicData.MarkDirty();
        part.WaitChannel.Writer.TryWrite(offset);
        return Task.FromResult(offset);
    }

    public Task<long[]> AppendBatchAsync(string topic, int partition, Message[] messages, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryGetValue(topic, out var topicData))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var part = topicData.Partitions[partition];
        var offsets = new long[messages.Length];

        lock (part.WriteLock)
        {
            for (int i = 0; i < messages.Length; i++)
            {
                var offset = part.NextOffset;
                offsets[i] = offset;
                messages[i].Offset = offset;
                messages[i].Topic = topic;
                messages[i].Partition = partition;
                part.Messages.Add(messages[i]);
                part.NextOffset = offset + 1;
            }
        }

        topicData.MarkDirty();
        part.WaitChannel.Writer.TryWrite(offsets[^1]);
        return Task.FromResult(offsets);
    }

    public Task<Message[]> ReadAsync(string topic, int partition, long offset, int count, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryGetValue(topic, out var topicData))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var part = topicData.Partitions[partition];
        var result = new List<Message>(count);

        lock (part.WriteLock)
        {
            if (offset >= part.NextOffset) return Task.FromResult(Array.Empty<Message>());
            if (offset < part.EarliestOffset) offset = part.EarliestOffset;

            foreach (var msg in part.Messages)
            {
                if (msg.Offset >= offset && result.Count < count)
                    result.Add(msg);
            }
        }

        return Task.FromResult(result.ToArray());
    }

    public long GetLatestOffset(string topic, int partition)
    {
        if (!_topics.TryGetValue(topic, out var topicData))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        return topicData.Partitions[partition].NextOffset;
    }

    public long GetEarliestOffset(string topic, int partition)
    {
        if (!_topics.TryGetValue(topic, out var topicData))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        return topicData.Partitions[partition].EarliestOffset;
    }

    public async Task WaitForMessagesAsync(string topic, int partition, long offset, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryGetValue(topic, out var topicData))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var part = topicData.Partitions[partition];
        if (part.NextOffset > offset) return;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        try
        {
            while (await part.WaitChannel.Reader.WaitToReadAsync(cts.Token))
            {
                while (part.WaitChannel.Reader.TryRead(out var notifiedOffset))
                {
                    if (notifiedOffset >= offset) return;
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // Timeout
        }
    }

    public async Task DeleteAsync(string topic, CancellationToken cancellationToken = default)
    {
        if (_topics.TryRemove(topic, out var topicData))
        {
            await topicData.DisposeAsync();
            var topicPath = Path.Combine(_basePath, topic);
            if (Directory.Exists(topicPath))
                Directory.Delete(topicPath, true);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var kvp in _topics)
        {
            await kvp.Value.DisposeAsync();
        }
        _topics.Clear();
    }
}
