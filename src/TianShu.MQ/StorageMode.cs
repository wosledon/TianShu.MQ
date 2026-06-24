namespace TianShu.MQ.Core;

/// <summary>
/// 消息存储模式
/// </summary>
public enum StorageMode
{
    /// <summary>内存模式，进程重启后丢失</summary>
    Memory,

    /// <summary>持久化模式，消息写入磁盘</summary>
    Persist
}
