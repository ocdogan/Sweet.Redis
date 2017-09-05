using System;

namespace Sweet.Redis
{
    public interface IRedisDb : IDisposable
    {
        bool Disposed { get; }
        Guid Id { get; }
        int Db { get; }

        RedisConnectionPool Pool { get; }

        IRedisConnectionCommands Connection { get; }
        IRedisHashesCommands Hashes { get; }
        IRedisKeysCommands Keys { get; }
        IRedisListsCommands Lists { get; }
        IRedisServerCommands Server { get; }
        IRedisSetsCommands Sets { get; }
        IRedisStringsCommands Strings { get; }
    }
}
