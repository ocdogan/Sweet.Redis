#region License
//  The MIT License (MIT)
//
//  Copyright (c) 2017, Cagatay Dogan
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//      The above copyright notice and this permission notice shall be included in
//      all copies or substantial portions of the Software.
//
//      THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//      IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//      FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//      AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//      LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//      OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//      THE SOFTWARE.
#endregion License

namespace Sweet.Redis
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
        public static readonly byte[] EX = "EX".ToBytes();
        public static readonly byte[] PX = "PX".ToBytes();
        public static readonly byte[] NX = "NX".ToBytes();
        public static readonly byte[] XX = "XX".ToBytes();

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

        // SlowLog command options
        public static readonly byte[] Len = "LEN".ToBytes();
        public static readonly byte[] Reset = "RESET".ToBytes();

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

        // Sorted Set commands
        public static readonly byte[] ZAdd = "ZADD".ToBytes();
        public static readonly byte[] ZCard = "ZCARD".ToBytes();
        public static readonly byte[] ZCount = "ZCOUNT".ToBytes();
        public static readonly byte[] ZIncrBy = "ZINCRBY".ToBytes();
        public static readonly byte[] ZInterStore = "ZINTERSTORE".ToBytes();
        public static readonly byte[] ZLexCount = "ZLEXCOUNT".ToBytes();
        public static readonly byte[] ZRange = "ZRANGE".ToBytes();
        public static readonly byte[] ZRangeByLex = "ZRANGEBYLEX".ToBytes();
        public static readonly byte[] ZRangeByScore = "ZRANGEBYSCORE".ToBytes();
        public static readonly byte[] ZRank = "ZRANK".ToBytes();
        public static readonly byte[] ZRem = "ZREM".ToBytes();
        public static readonly byte[] ZRemRangeByLex = "ZREMRANGEBYLEX".ToBytes();
        public static readonly byte[] ZRemRangeByRank = "ZREMRANGEBYRANK".ToBytes();
        public static readonly byte[] ZRemRangeByScore = "ZREMRANGEBYSCORE".ToBytes();
        public static readonly byte[] ZRevRange = "ZREVRANGE".ToBytes();
        public static readonly byte[] ZRevRangeByScore = "ZREVRANGEBYSCORE".ToBytes();
        public static readonly byte[] ZRevRank = "ZREVRANK".ToBytes();
        public static readonly byte[] ZScan = "ZSCAN".ToBytes();
        public static readonly byte[] ZScore = "ZSCORE".ToBytes();
        public static readonly byte[] ZUnionStore = "ZUNIONSTORE".ToBytes();

        // Sorted Set Add command options
        public static readonly byte[] CH = "CH".ToBytes();

        // Sorted Set command options
        public static readonly byte[] Weights = "WEIGHTS".ToBytes();
        public static readonly byte[] WithScores = "WITHSCORES".ToBytes();
        public static readonly byte[] Count = "COUNT".ToBytes();
        public static readonly byte[] Aggregate = "AGGREGATE".ToBytes();

        // Sorted Set command Aggregate options
        public static readonly byte[] Sum = "SUM".ToBytes();
        public static readonly byte[] Min = "MIN".ToBytes();
        public static readonly byte[] Max = "MAX".ToBytes();

        // Sorted Set Scan command options
        public static readonly byte[] Match = "MATCH".ToBytes();

        // HyperLogLog commands
        public static readonly byte[] PfAdd = "PFADD".ToBytes();
        public static readonly byte[] PfCount = "PFCOUNT".ToBytes();
        public static readonly byte[] PfMerge = "PFMERGE".ToBytes();

        // Scripting commands
        public static readonly byte[] Eval = "EVAL".ToBytes();
        public static readonly byte[] EvalSha = "EVALSHA".ToBytes();

        public static readonly byte[] Script = "SCRIPT".ToBytes();

        // Scripting Script command options
        public static readonly byte[] Flush = "FLUSH".ToBytes();
        public static readonly byte[] Load = "LOAD".ToBytes();
        public static readonly byte[] Debug = "DEBUG".ToBytes();

        // PubSub commands
        public static readonly byte[] PSubscribe = "PSUBSCRIBE".ToBytes();
        public static readonly byte[] Publish = "PUBLISH".ToBytes();
        public static readonly byte[] PubSub = "PUBSUB".ToBytes();
        public static readonly byte[] PUnsubscribe = "PUNSUBSCRIBE".ToBytes();
        public static readonly byte[] Subscribe = "SUBSCRIBE".ToBytes();
        public static readonly byte[] Unsubscribe = "UNSUBSCRIBE".ToBytes();

        // PubSub command options
        public static readonly byte[] Channels = "CHANNELS".ToBytes();
        public static readonly byte[] NumSub = "NUMSUB".ToBytes();
        public static readonly byte[] NumPat = "NUMPAT".ToBytes();

        // Geo commands
        public static readonly byte[] GeoAdd = "GEOADD".ToBytes();
        public static readonly byte[] GeoDist = "GEODIST".ToBytes();
        public static readonly byte[] GeoHash = "GEOHASH".ToBytes();
        public static readonly byte[] GeoPos = "GEOPOS".ToBytes();
        public static readonly byte[] GeoRadius = "GEORADIUS".ToBytes();
        public static readonly byte[] GeoRadiusByMember = "GEORADIUSBYMEMBER".ToBytes();

        // Geo commands options
        public static readonly byte[] Feet = "ft".ToBytes();
        public static readonly byte[] Kilometers = "km".ToBytes();
        public static readonly byte[] Meters = "m".ToBytes();
        public static readonly byte[] Miles = "mi".ToBytes();
        public static readonly byte[] WithCoord = "WITHCOORD".ToBytes();
        public static readonly byte[] WithDist = "WITHDIST".ToBytes();
        public static readonly byte[] WithHash = "WITHHASH".ToBytes();
        public static readonly byte[] Ascending = "ASC".ToBytes();
        public static readonly byte[] Store = "STORE".ToBytes();
        public static readonly byte[] StoreDist = "STOREDIST".ToBytes();

        // Sentinel commands
        public readonly static byte[] Sentinel = "SENTINEL".ToBytes();
        public readonly static byte[] SentinelCheckQuorum = "ckquorum".ToBytes();
        public readonly static byte[] SentinelFailover = "failover".ToBytes();
        public readonly static byte[] SentinelFlushConfig = "flushconfig".ToBytes();
        public readonly static byte[] SentinelGetMasterAddrByName = "get-master-addr-by-name".ToBytes();
        public readonly static byte[] SentinelMaster = "master".ToBytes();
        public readonly static byte[] SentinelMasters = "masters".ToBytes();
        public readonly static byte[] SentinelMonitor = "MONITOR".ToBytes();
        public readonly static byte[] SentinelRemove = "REMOVE".ToBytes();
        public readonly static byte[] SentinelReset = "reset".ToBytes();
        public readonly static byte[] Sentinels = "sentinels".ToBytes();
        public readonly static byte[] SentinelSet = "SET".ToBytes();
        public readonly static byte[] SentinelSlaves = "slaves".ToBytes();

        // Sentinel message channels
        public readonly static byte[] SentinelChanelResetMaster = "+reset-master".ToBytes();
        public readonly static byte[] SentinelChanelSlave = "+slave".ToBytes();
        public readonly static byte[] SentinelChanelFailoverStateReconfSlaves = "+failover-state-reconf-slaves".ToBytes();
        public readonly static byte[] SentinelChanelFailoverDetected = "+failover-detected".ToBytes();
        public readonly static byte[] SentinelChanelSlaveReconfSent = "+slave-reconf-sent".ToBytes();
        public readonly static byte[] SentinelChanelSlaveReconfInprog = "+slave-reconf-inprog".ToBytes();
        public readonly static byte[] SentinelChanelSlaveReconfDone = "+slave-reconf-done".ToBytes();
        public readonly static byte[] SentinelChanelDupSentinel = "-dup-sentinel".ToBytes();
        public readonly static byte[] SentinelChanelSentinel = "+sentinel".ToBytes();
        public readonly static byte[] SentinelChanelSDownEntered = "+sdown".ToBytes();
        public readonly static byte[] SentinelChanelSDownExited = "-sdown".ToBytes();
        public readonly static byte[] SentinelChanelODownEntered = "+odown".ToBytes();
        public readonly static byte[] SentinelChanelODownExited = "-odown".ToBytes();
        public readonly static byte[] SentinelChanelNewEpoch = "+new-epoch".ToBytes();
        public readonly static byte[] SentinelChanelTryFailover = "+try-failover".ToBytes();
        public readonly static byte[] SentinelChanelElectedLeader = "+elected-leader".ToBytes();
        public readonly static byte[] SentinelChanelFailoverStateSelectSlave = "+failover-state-select-slave".ToBytes();
        public readonly static byte[] SentinelChanelNoGoodSlave = "no-good-slave".ToBytes();
        public readonly static byte[] SentinelChanelSelectedSlave = "selected-slave".ToBytes();
        public readonly static byte[] SentinelChanelFailoverStateSendSlaveofNoOne = "failover-state-send-slaveof-noone".ToBytes();
        public readonly static byte[] SentinelChanelFailoverEntForTimeout = "failover-end-for-timeout".ToBytes();
        public readonly static byte[] SentinelChanelFailoverEnd = "failover-end".ToBytes();
        public readonly static byte[] SentinelChanelSwitchMaster = "switch-master".ToBytes();
        public readonly static byte[] SentinelChanelTiltEntered = "+tilt".ToBytes();
        public readonly static byte[] SentinelChanelTiltExited = "-tilt".ToBytes();

        // Transaction commands
        public readonly static byte[] Multi = "MULTI".ToBytes();
        public readonly static byte[] Exec = "EXEC".ToBytes();
        public readonly static byte[] Discard = "DISCARD".ToBytes();
    }
}