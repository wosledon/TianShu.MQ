using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TianShu.MQ.Core;
using TianShu.MQ.Storage;

namespace TianShu.MQ.Persist;

/// <summary>
/// 基于追加写日志的持久化存储引擎
/// 每个分区独立文件：{topicPath}/{partition}/data.log
/// 每条记录格式：[8字节offset][8字节bodyLength][utf8-body][8字节timestamp][4字节headLength][headers-json]
/// 恢复时按日志文件重放构建内存索引
/// </summary>
public sealed class PersistStorageEngine : IStorageEngine
{
    private readonly string _basePath;
    private readonly TimeSpan _flushInterval;
    private readonly FlushStrategy _flushStrategy;
    private readonly ConcurrentDictionary<string, TopicData> _topics = new();

    public PersistStorageEngine(string basePath, FlushStrategy flushStrategy = FlushStrategy.Periodic, TimeSpan? flushInterval = null)
    {
        _basePath = basePath;
        _flushStrategy = flushStrategy;
        _flushInterval = flushInterval ?? TimeSpan.FromMilliseconds(100);
        Directory.CreateDirectory(_basePath);
    }

    /// <summary>
    /// 偏移量持久化管理器
    /// </summary>
    public sealed class OffsetStore
    {
        private readonly string _filePath;
        private readonly object _lock = new();
        private Dictionary<string, Dictionary<int, long>> _offsets = new();
        private bool _dirty;

        public OffsetStore(string basePath)
        {
            _filePath = Path.Combine(basePath, "_offsets.json");
            Load();
        }

        public long GetCommittedOffset(string topic, string groupId, int partition)
        {
            lock (_lock)
            {
                if (_offsets.TryGetValue($"{topic}:{groupId}", out var parts))
                    return parts.GetValueOrDefault(partition, 0);
                return 0;
            }
        }

        public void CommitOffset(string topic, string groupId, int partition, long offset)
        {
            lock (_lock)
            {
                var key = $"{topic}:{groupId}";
                if (!_offsets.TryGetValue(key, out var parts))
                {
                    parts = new Dictionary<int, long>();
                    _offsets[key] = parts;
                }

                if (!parts.TryGetValue(partition, out var existing) || offset > existing)
                {
                    parts[partition] = offset;
                    _dirty = true;
                }
            }
        }

        public void Flush()
        {
            if (!_dirty) return;
            lock (_lock)
            {
                try
                {
                    var json = JsonSerializer.Serialize(_offsets);
                    File.WriteAllText(_filePath, json);
                    _dirty = false;
                }
                catch
                {
                    // Log error
                }
            }
        }

        private void Load()
        {
            if (!File.Exists(_filePath)) return;
            try
            {
                var json = File.ReadAllText(_filePath);
                var data = JsonSerializer.Deserialize<Dictionary<string, Dictionary<int, long>>>(json);
                if (data != null) _offsets = data;
            }
            catch
            {
                // Corrupted, start fresh
            }
        }
    }

    // ===== Partition 日志格式 =====
    // [offset:8B][bodyLen:4B][body:bodyLen][ts:8B][headerLen:2B][headersJson:headerLen]
    private static readonly byte[] NewLine = { (byte)'\n' };

    private sealed class PartitionData
    {
#pragma warning disable CS0649
        public long EarliestOffset = 0;
#pragma warning restore CS0649
        public long NextOffset;
        public readonly List<Message> Messages = new();
        public readonly Channel<long> WaitChannel;
        public readonly object WriteLock = new();
        public readonly string LogFilePath;
        public FileStream? LogStream;
        public long BytesWritten;

        public PartitionData(string logFilePath)
        {
            LogFilePath = logFilePath;
            WaitChannel = Channel.CreateBounded<long>(new BoundedChannelOptions(1024)
            {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.DropOldest
            });
        }

        public void OpenLog()
        {
            var dir = Path.GetDirectoryName(LogFilePath)!;
            Directory.CreateDirectory(dir);
            LogStream = new FileStream(LogFilePath, FileMode.Append, FileAccess.Write, FileShare.Read, 8192, true);
        }

        public void AppendToLog(Message msg)
        {
            if (LogStream == null) return;

            var bodyBytes = msg.Body ?? Array.Empty<byte>();
            var headerJson = JsonSerializer.Serialize(msg.Headers);
            var headerBytes = Encoding.UTF8.GetBytes(headerJson);

            // 写入日志行：消息体长度 + 消息体 JSON + 换行
            // 简化格式：每条消息一行 JSON
            var logEntry = JsonSerializer.Serialize(new
            {
                msg.MessageId,
                Body = Convert.ToBase64String(bodyBytes),
                msg.PartitionKey,
                msg.Offset,
                Headers = msg.Headers,
                Timestamp = msg.Timestamp,
                msg.ScheduledEnqueueTime,
                msg.Topic,
                msg.Partition
            });

            var lineBytes = Encoding.UTF8.GetBytes(logEntry + "\n");
            LogStream.Write(lineBytes, 0, lineBytes.Length);
            BytesWritten += lineBytes.Length;
        }

        public void FlushLog()
        {
            LogStream?.Flush(true);
        }

        public void CloseLog()
        {
            LogStream?.Flush(true);
            LogStream?.Close();
            LogStream?.Dispose();
            LogStream = null;
        }
    }

    private sealed class TopicData : IAsyncDisposable
    {
        public readonly PartitionData[] Partitions;
        public readonly Timer? FlushTimer;
        private readonly Action? _flushAction;

        public TopicData(int partitionCount, string topicPath, TimeSpan flushInterval, FlushStrategy strategy)
        {
            Partitions = new PartitionData[partitionCount];
            for (int i = 0; i < partitionCount; i++)
            {
                var logPath = Path.Combine(topicPath, i.ToString(), "data.log");
                Partitions[i] = new PartitionData(logPath);
            }

            if (strategy == FlushStrategy.Periodic)
            {
                _flushAction = () =>
                {
                    foreach (var p in Partitions)
                    {
                        lock (p.WriteLock) p.FlushLog();
                    }
                };
                FlushTimer = new Timer(_ => _flushAction(), null, flushInterval, flushInterval);
            }
        }

        public void OpenAllLogs()
        {
            foreach (var p in Partitions)
                p.OpenLog();
        }

        public ValueTask DisposeAsync()
        {
            FlushTimer?.Dispose();
            foreach (var p in Partitions)
            {
                lock (p.WriteLock) p.CloseLog();
            }
            return ValueTask.CompletedTask;
        }
    }

    public Task InitializeAsync(string topic, int partitions, int? capacity = null, CancellationToken cancellationToken = default)
    {
        var topicPath = Path.Combine(_basePath, topic);
        Directory.CreateDirectory(topicPath);

        var topicData = new TopicData(partitions, topicPath, _flushInterval, _flushStrategy);

        // 恢复已有数据：从每个分区的日志文件中回放
        for (int p = 0; p < partitions; p++)
        {
            var logFile = Path.Combine(topicPath, p.ToString(), "data.log");
            if (File.Exists(logFile))
            {
                try
                {
                    var part = topicData.Partitions[p];
                    var lines = File.ReadAllLines(logFile);
                    foreach (var line in lines)
                    {
                        if (string.IsNullOrWhiteSpace(line)) continue;
                        try
                        {
                            var doc = JsonDocument.Parse(line);
                            var root = doc.RootElement;

                            var msg = new Message
                            {
                                MessageId = root.GetProperty("MessageId").GetString() ?? Guid.NewGuid().ToString("N"),
                                Body = Convert.FromBase64String(root.GetProperty("Body").GetString() ?? ""),
                                PartitionKey = root.GetProperty("PartitionKey").GetString() ?? "",
                                Offset = part.NextOffset,
                                Timestamp = root.GetProperty("Timestamp").GetDateTime(),
                                Topic = topic,
                                Partition = p
                            };

                            if (root.TryGetProperty("ScheduledEnqueueTime", out var scheduled) && scheduled.ValueKind != System.Text.Json.JsonValueKind.Null)
                                msg.ScheduledEnqueueTime = scheduled.GetDateTime();

                            if (root.TryGetProperty("Headers", out var headersProp) && headersProp.ValueKind == System.Text.Json.JsonValueKind.Object)
                            {
                                foreach (var h in headersProp.EnumerateObject())
                                {
                                    msg.Headers[h.Name] = h.Value.GetString() ?? "";
                                }
                            }

                            part.Messages.Add(msg);
                            part.NextOffset = msg.Offset + 1;
                        }
                        catch
                        {
                            // Skip corrupted line
                        }
                    }
                }
                catch
                {
                    // Corrupted log file, start fresh
                }
            }
        }

        if (!_topics.TryAdd(topic, topicData))
            throw new InvalidOperationException($"Topic '{topic}' already exists.");

        // 恢复完成后打开日志文件
        topicData.OpenAllLogs();

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

            // 追加写入日志
            part.AppendToLog(message);

            if (_flushStrategy == FlushStrategy.PerWrite)
                part.FlushLog();
        }

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
                part.AppendToLog(messages[i]);
            }

            if (_flushStrategy == FlushStrategy.PerWrite)
                part.FlushLog();
        }

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
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) { }
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
            await kvp.Value.DisposeAsync();
        _topics.Clear();
    }
}
