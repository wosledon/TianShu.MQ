using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using TianShu.MQ.Core;
using TianShu.MQ.Persist;
using Xunit;

namespace TianShu.MQ.Tests;

public class PersistStorageTests : IDisposable
{
    private readonly string _testPath;

    public PersistStorageTests()
    {
        _testPath = Path.Combine(Path.GetTempPath(), $"tianshu_mq_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testPath);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testPath))
        {
            Directory.Delete(_testPath, true);
        }
    }

    [Fact]
    public async Task PersistEngine_ShouldWriteAndRead()
    {
        await using var engine = new PersistStorageEngine(_testPath);
        await engine.InitializeAsync("test-topic", 2);

        var msg = new Message
        {
            Body = Encoding.UTF8.GetBytes("hello persist"),
            PartitionKey = "k1"
        };

        var offset = await engine.AppendAsync("test-topic", 0, msg);
        Assert.Equal(0, offset);

        var messages = await engine.ReadAsync("test-topic", 0, 0, 10);
        Assert.Single(messages);
        Assert.Equal("hello persist", Encoding.UTF8.GetString(messages[0].Body));
    }

    [Fact]
    public async Task PersistEngine_BatchAppend_ShouldWork()
    {
        await using var engine = new PersistStorageEngine(_testPath);
        await engine.InitializeAsync("batch-topic", 1);

        var messages = new Message[5];
        for (int i = 0; i < 5; i++)
        {
            messages[i] = new Message
            {
                Body = Encoding.UTF8.GetBytes($"msg-{i}"),
                PartitionKey = "k"
            };
        }

        var offsets = await engine.AppendBatchAsync("batch-topic", 0, messages);
        Assert.Equal(5, offsets.Length);
        Assert.Equal(0, offsets[0]);
        Assert.Equal(4, offsets[4]);

        var read = await engine.ReadAsync("batch-topic", 0, 0, 10);
        Assert.Equal(5, read.Length);
    }

    [Fact]
    public async Task PersistEngine_Recovery_ShouldRestoreData()
    {
        // 写入数据
        var engine1 = new PersistStorageEngine(_testPath, FlushStrategy.Periodic, TimeSpan.FromMilliseconds(50));
        await engine1.InitializeAsync("recovery-topic", 1);

        for (int i = 0; i < 3; i++)
        {
            await engine1.AppendAsync("recovery-topic", 0, new Message
            {
                Body = Encoding.UTF8.GetBytes($"msg-{i}"),
                PartitionKey = "k"
            });
        }

        // 等待刷盘
        await Task.Delay(200);
        await engine1.DisposeAsync();

        // 新引擎读取恢复
        var engine2 = new PersistStorageEngine(_testPath);
        await engine2.InitializeAsync("recovery-topic", 1);

        var latest = engine2.GetLatestOffset("recovery-topic", 0);
        Assert.Equal(3, latest);

        var messages = await engine2.ReadAsync("recovery-topic", 0, 0, 10);
        Assert.Equal(3, messages.Length);

        await engine2.DisposeAsync();
    }

    [Fact]
    public async Task PersistEngine_MultiplePartitions_ShouldWork()
    {
        await using var engine = new PersistStorageEngine(_testPath);
        await engine.InitializeAsync("multi-part", 3);

        for (int p = 0; p < 3; p++)
        {
            for (int i = 0; i < 5; i++)
            {
                await engine.AppendAsync("multi-part", p, new Message
                {
                    Body = Encoding.UTF8.GetBytes($"p{p}-msg{i}"),
                    PartitionKey = $"key{p}"
                });
            }
        }

        for (int p = 0; p < 3; p++)
        {
            var latest = engine.GetLatestOffset("multi-part", p);
            Assert.Equal(5, latest);

            var msgs = await engine.ReadAsync("multi-part", p, 0, 10);
            Assert.Equal(5, msgs.Length);
        }
    }
}
