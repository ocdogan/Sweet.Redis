using System;

namespace Sweet.Redis
{
    public interface IRedisDb : IDisposable
    {
        bool Disposed { get; }
        Guid Id { get; }
        int Db { get; }
        bool ThrowOnError { get; }

        RedisConnectionPool Pool { get; }

        IRedisConnectionCommands Connection { get; }
        IRedisHashesCommands Hashes { get; }
        IRedisHyperLogLogCommands HyperLogLogCommands { get; }
        IRedisKeysCommands Keys { get; }
        IRedisListsCommands Lists { get; }
        IRedisScriptingCommands Scripting { get; }
        IRedisServerCommands Server { get; }
        IRedisSetsCommands Sets { get; }
        IRedisSortedSetsCommands SortedSets { get; }
        IRedisStringsCommands Strings { get; }
    }
}
