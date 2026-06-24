using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using TianShu.MQ.Core;

namespace TianShu.MQ.Storage;

/// <summary>
/// 基于 Channel + RingBuffer 的内存存储引擎（高性能实现）
/// </summary>
public sealed class MemoryStorageEngine : IStorageEngine
{
    /// <summary>
    /// 每个分区的数据结构，使用数组实现 RingBuffer，避免 ConcurrentQueue 的链表分配
    /// </summary>
    private sealed class PartitionData
    {
        private readonly Message[] _buffer;
        private readonly int _capacity;
        private long _writeIndex;      // 下一个写入位置（在 buffer 中的位置）
        private long _nextOffset;      // 下一个 offset
        private long _earliestOffset;  // 最早可用的 offset
        private readonly Channel<long> _waitChannel;
        private readonly object _writeLock = new();

        public PartitionData(int capacity)
        {
            _capacity = capacity > 0 ? capacity : 1_000_000;
            _buffer = new Message[_capacity];
            _nextOffset = 0;
            _earliestOffset = 0;
            _writeIndex = 0;
            _waitChannel = Channel.CreateBounded<long>(new BoundedChannelOptions(1024)
            {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.DropOldest
            });
        }

        public long Append(Message message)
        {
            long offset;
            lock (_writeLock)
            {
                offset = _nextOffset;
                var bufferIndex = (int)(offset % _capacity);
                _buffer[bufferIndex] = message;
                _nextOffset = offset + 1;
                _writeIndex = bufferIndex + 1;

                // 如果写满了，推进最早 offset（RingBuffer 覆盖）
                if (_nextOffset - _earliestOffset > _capacity)
                {
                    _earliestOffset = _nextOffset - _capacity;
                }
            }

            // 通知等待者
            _waitChannel.Writer.TryWrite(offset);
            return offset;
        }

        public long[] AppendBatch(Message[] messages)
        {
            var offsets = new long[messages.Length];
            lock (_writeLock)
            {
                for (int i = 0; i < messages.Length; i++)
                {
                    var offset = _nextOffset;
                    offsets[i] = offset;
                    var bufferIndex = (int)(offset % _capacity);
                    _buffer[bufferIndex] = messages[i];
                    _nextOffset = offset + 1;

                    if (_nextOffset - _earliestOffset > _capacity)
                    {
                        _earliestOffset = _nextOffset - _capacity;
                    }
                }
            }

            // 只通知一次
            _waitChannel.Writer.TryWrite(offsets[^1]);
            return offsets;
        }

        public Message[] Read(long offset, int count)
        {
            var result = new List<Message>(count);
            lock (_writeLock)
            {
                // offset 超出范围
                if (offset >= _nextOffset)
                    return Array.Empty<Message>();

                // offset 已被覆盖
                if (offset < _earliestOffset)
                    offset = _earliestOffset;

                var end = Math.Min(offset + count, _nextOffset);
                for (long o = offset; o < end; o++)
                {
                    var bufferIndex = (int)(o % _capacity);
                    var msg = _buffer[bufferIndex];
                    if (msg != null)
                        result.Add(msg);
                }
            }
            return result.ToArray();
        }

        public long NextOffset
        {
            get { lock (_writeLock) return _nextOffset; }
        }

        public long EarliestOffset
        {
            get { lock (_writeLock) return _earliestOffset; }
        }

        public async Task WaitForMessagesAsync(long offset, TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (NextOffset > offset) return;

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(timeout);

            try
            {
                while (await _waitChannel.Reader.WaitToReadAsync(cts.Token))
                {
                    while (_waitChannel.Reader.TryRead(out var notifiedOffset))
                    {
                        if (notifiedOffset >= offset)
                            return;
                    }
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // Timeout - expected
            }
        }
    }

    private readonly ConcurrentDictionary<string, PartitionData[]> _topics = new();

    public Task InitializeAsync(string topic, int partitions, CancellationToken cancellationToken = default)
    {
        var parts = new PartitionData[partitions];
        for (int i = 0; i < partitions; i++)
        {
            parts[i] = new PartitionData(capacity: 0);
        }

        if (!_topics.TryAdd(topic, parts))
            throw new InvalidOperationException($"Topic '{topic}' already exists.");

        return Task.CompletedTask;
    }

    public Task<long> AppendAsync(string topic, int partition, Message message, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryGetValue(topic, out var parts))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var part = parts[partition];
        message.Offset = part.Append(message);
        message.Topic = topic;
        message.Partition = partition;

        return Task.FromResult(message.Offset);
    }

    public Task<long[]> AppendBatchAsync(string topic, int partition, Message[] messages, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryGetValue(topic, out var parts))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var part = parts[partition];
        var offsets = part.AppendBatch(messages);

        for (int i = 0; i < messages.Length; i++)
        {
            messages[i].Offset = offsets[i];
            messages[i].Topic = topic;
            messages[i].Partition = partition;
        }

        return Task.FromResult(offsets);
    }

    public Task<Message[]> ReadAsync(string topic, int partition, long offset, int count, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryGetValue(topic, out var parts))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        var part = parts[partition];
        var result = part.Read(offset, count);
        return Task.FromResult(result);
    }

    public long GetLatestOffset(string topic, int partition)
    {
        if (!_topics.TryGetValue(topic, out var parts))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        return parts[partition].NextOffset;
    }

    public long GetEarliestOffset(string topic, int partition)
    {
        if (!_topics.TryGetValue(topic, out var parts))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        return parts[partition].EarliestOffset;
    }

    public Task WaitForMessagesAsync(string topic, int partition, long offset, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        if (!_topics.TryGetValue(topic, out var parts))
            throw new InvalidOperationException($"Topic '{topic}' does not exist.");

        return parts[partition].WaitForMessagesAsync(offset, timeout, cancellationToken);
    }

    public Task DeleteAsync(string topic, CancellationToken cancellationToken = default)
    {
        _topics.TryRemove(topic, out _);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _topics.Clear();
        return ValueTask.CompletedTask;
    }
}
