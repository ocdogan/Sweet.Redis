﻿namespace Sweet.Redis
{
    public static class RedisCommands
    {
        public static readonly byte[] EmptyString = "".ToBytes();

        // Basic Commands
        public static readonly byte[] Auth = "AUTH".ToBytes();
        public static readonly byte[] Echo = "ECHO".ToBytes();
        public static readonly byte[] Ping = "PING".ToBytes();
        public static readonly byte[] Quit = "QUIT".ToBytes();
        public static readonly byte[] Select = "SELECT".ToBytes();
        public static readonly byte[] SwapDb = "SWAPDB".ToBytes();

        // Strings Commands
        public static readonly byte[] Append = "APPEND".ToBytes();

        public static readonly byte[] BitCount = "BITCOUNT".ToBytes();

        public static readonly byte[] Decr = "DECS".ToBytes();
        public static readonly byte[] DecrBy = "DECRBY".ToBytes();

        public static readonly byte[] Get = "GET".ToBytes();
        public static readonly byte[] GetBit = "GETBIT".ToBytes();
        public static readonly byte[] GetRange = "GETRANGE".ToBytes();
        public static readonly byte[] GetSet = "GETSET".ToBytes();

        public static readonly byte[] Incr = "INCR".ToBytes();
        public static readonly byte[] IncrBy = "INCRBY".ToBytes();
        public static readonly byte[] IncrByFloat = "INCRBYFLOAT".ToBytes();

        public static readonly byte[] MGet = "MGET".ToBytes();

        public static readonly byte[] MSet = "MSET".ToBytes();
        public static readonly byte[] MSetNx = "MSETNX".ToBytes();
        public static readonly byte[] PSetEx = "PSETEX".ToBytes();

        public static readonly byte[] Set = "SET".ToBytes();
        public static readonly byte[] SetBit = "SETBIT".ToBytes();
        public static readonly byte[] SetEx = "SETEX".ToBytes();
        public static readonly byte[] SetNx = "SETNX".ToBytes();
        public static readonly byte[] SetRange = "SETRANGE".ToBytes();

        public static readonly byte[] StrLen = "STRLEN".ToBytes();

        // Set additional commands
        public static readonly byte[] Ex = "EX".ToBytes();
        public static readonly byte[] Px = "PX".ToBytes();
        public static readonly byte[] Nx = "NX".ToBytes();
        public static readonly byte[] Xx = "XX".ToBytes();

        // List commands
        public static readonly byte[] BLPop = "BLPOP".ToBytes();
        public static readonly byte[] BRPop = "BRPOP".ToBytes();
        public static readonly byte[] BRPopLPush = "BRPOPLPUSH".ToBytes();

        public static readonly byte[] LIndex = "LINDEX".ToBytes();
        public static readonly byte[] LInsert = "LINSERT".ToBytes();
        public static readonly byte[] LLen = "LLEN".ToBytes();
        public static readonly byte[] LPop = "LPOP".ToBytes();
        public static readonly byte[] LPush = "LPUSH".ToBytes();
        public static readonly byte[] LPushX = "LPUSHX".ToBytes();
        public static readonly byte[] LRange = "LRANGE".ToBytes();
        public static readonly byte[] LRem = "LREM".ToBytes();
        public static readonly byte[] LSet = "LSET".ToBytes();
        public static readonly byte[] LTrim = "LTRIM".ToBytes();

        public static readonly byte[] RPop = "RPOP".ToBytes();
        public static readonly byte[] RPopLPush = "RPOPLPUSH".ToBytes();
        public static readonly byte[] RPush = "RPUSH".ToBytes();
        public static readonly byte[] RPushX = "RPUSHX".ToBytes();

        // LInsert modes
        public static readonly byte[] Before = "BEFORE".ToBytes();
        public static readonly byte[] After = "AFTER".ToBytes();

        // Key commands
        public static readonly byte[] Del = "DEL".ToBytes();
        public static readonly byte[] Dump = "DUMP".ToBytes();
        public static readonly byte[] Exists = "EXISTS".ToBytes();
        public static readonly byte[] Expire = "EXPIRE".ToBytes();
        public static readonly byte[] ExpireAt = "EXPIREAT".ToBytes();
        public static readonly byte[] Keys = "KEYS".ToBytes();
        public static readonly byte[] Migrate = "MIGRATE".ToBytes();
        public static readonly byte[] Move = "MOVE".ToBytes();
        public static readonly byte[] Object = "OBJECT".ToBytes();
        public static readonly byte[] Persist = "PERSIST".ToBytes();
        public static readonly byte[] PExpire = "PEXPIRE".ToBytes();
        public static readonly byte[] PExpireAt = "PEXPIREAT".ToBytes();
        public static readonly byte[] PTtl = "PTTL".ToBytes();
        public static readonly byte[] RandomKey = "RANDOMKEY".ToBytes();
        public static readonly byte[] Rename = "RENAME".ToBytes();
        public static readonly byte[] RenameNx = "RENAMENX".ToBytes();
        public static readonly byte[] Restore = "RESTORE".ToBytes();
        public static readonly byte[] ScanLoop = "SCANLOOP".ToBytes();
        public static readonly byte[] Sort = "SORT".ToBytes();
        public static readonly byte[] Touch = "TOUCH".ToBytes();
        public static readonly byte[] Ttl = "TTL".ToBytes();
        public static readonly byte[] Type = "TYPE".ToBytes();

        // Migrate command options
        public static readonly byte[] Copy = "COPY".ToBytes();
        public static readonly byte[] Replace = "REPLACE".ToBytes();

        // Object command options
        public static readonly byte[] RefCount = "REFCOUNT".ToBytes();
        public static readonly byte[] Encoding = "ENCODING".ToBytes();
        public static readonly byte[] IdleTime = "IDLETIME".ToBytes();

        // Sort command options
        public static readonly byte[] Alpha = "ALPHA".ToBytes();
        public static readonly byte[] Descending = "DESC".ToBytes();
        public static readonly byte[] Limit = "LIMIT".ToBytes();
        public static readonly byte[] By = "BY".ToBytes();

        // Set commands
        public static readonly byte[] SAdd = "SADD".ToBytes();
        public static readonly byte[] SCard = "SCARD".ToBytes();
        public static readonly byte[] SDiff = "SDIFF".ToBytes();
        public static readonly byte[] SDiffStore = "SDIFFSTORE".ToBytes();
        public static readonly byte[] SInter = "SINTER".ToBytes();
        public static readonly byte[] SInterStore = "SINTERSTORE".ToBytes();
        public static readonly byte[] SIsMember = "SISMEMBER".ToBytes();
        public static readonly byte[] SMembers = "SMEMBERS".ToBytes();
        public static readonly byte[] SMove = "SMOVE".ToBytes();
        public static readonly byte[] SPop = "SPOP".ToBytes();
        public static readonly byte[] SRandMember = "SRANDMEMBER".ToBytes();
        public static readonly byte[] SRem = "SREM".ToBytes();
        public static readonly byte[] SScan = "SSCAN".ToBytes();
        public static readonly byte[] SUnion = "SUNION".ToBytes();
        public static readonly byte[] SUnionStore = "SUNIONSTORE".ToBytes();

        // Hash commands
        public static readonly byte[] HDel = "HDEL".ToBytes();
        public static readonly byte[] HExists = "HEXISTS".ToBytes();
        public static readonly byte[] HGet = "HGET".ToBytes();
        public static readonly byte[] HGetAll = "HGETALL".ToBytes();
        public static readonly byte[] HIncrBy = "HINCRBY".ToBytes();
        public static readonly byte[] HIncrByFloat = "HINCRBYFLOAT".ToBytes();
        public static readonly byte[] HKeys = "HKEYS".ToBytes();
        public static readonly byte[] HLen = "HLEN".ToBytes();
        public static readonly byte[] HMGet = "HMGET".ToBytes();
        public static readonly byte[] HMSet = "HMSET".ToBytes();
        public static readonly byte[] HScan = "HSCAN".ToBytes();
        public static readonly byte[] HSet = "HSET".ToBytes();
        public static readonly byte[] HSetNx = "HSETNX".ToBytes();
        public static readonly byte[] HStrLen = "HSTRLEN".ToBytes();
        public static readonly byte[] HVals = "HVALS".ToBytes();

        // Server commands
        public static readonly byte[] BGRewriteAOF = "BGREWRITEAOF".ToBytes();
        public static readonly byte[] BGSave = "BGSAVE".ToBytes();
        public static readonly byte[] Client = "CLIENT".ToBytes();
        public static readonly byte[] Config = "CONFIG".ToBytes();
        public static readonly byte[] DbSize = "DBSIZE".ToBytes();
        public static readonly byte[] FlushAll = "FLUSHALL".ToBytes();
        public static readonly byte[] FlushDb = "FLUSHDB".ToBytes();
        public static readonly byte[] Info = "INFO".ToBytes();
        public static readonly byte[] LastSave = "LASTSAVE".ToBytes();
        public static readonly byte[] Monitor = "MONITOR".ToBytes();
        public static readonly byte[] Save = "SAVE".ToBytes();
        public static readonly byte[] ShutDown = "SHUTDOWN".ToBytes();
        public static readonly byte[] SlaveOf = "SLAVEOF".ToBytes();
        public static readonly byte[] SlowLog = "SLOWLOG".ToBytes();
        public static readonly byte[] Sync = "SYNC".ToBytes();
        public static readonly byte[] Time = "TIME".ToBytes();

        // Client command options
        public static readonly byte[] GetName = "GETNAME".ToBytes();
        public static readonly byte[] Kill = "KILL".ToBytes();
        public static readonly byte[] List = "LIST".ToBytes();
        public static readonly byte[] Pause = "PAUSE".ToBytes();
        public static readonly byte[] Reply = "REPLY".ToBytes();
        public static readonly byte[] SetName = "SETNAME".ToBytes();

        // Cliend kill command options
        public static readonly byte[] Addr = "ADDR".ToBytes();
        public static readonly byte[] Id = "ID".ToBytes();
        public static readonly byte[] SkipMe = "SKIPME".ToBytes();
        public static readonly byte[] Yes = "YES".ToBytes();
        public static readonly byte[] No = "NO".ToBytes();

        // Client reply options
        public static readonly byte[] On = "ON".ToBytes();
        public static readonly byte[] Off = "OFF".ToBytes();
        public static readonly byte[] Skip = "SKIP".ToBytes();

        // Config command options
        public static readonly byte[] ResetStat = "RESETSTAT".ToBytes();
        public static readonly byte[] Rewrite = "REWRITE".ToBytes();

        // Flush command options
        public static readonly byte[] Async = "ASYNC".ToBytes();

        // Shutdown command options
        public static readonly byte[] NoSave = "NOSAVE".ToBytes();

        // SlaveOf command options
        public static readonly byte[] NoOne = "NO ONE".ToBytes();
    }
}