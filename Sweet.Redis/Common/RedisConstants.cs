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

using System.Collections.Generic;
using System.Globalization;

namespace Sweet.Redis
{
    public static class RedisConstants
    {
        #region Static Members

        public static readonly byte[] EmptyBytes = new byte[0];
        public static readonly byte[] LineEnd = CRLF.ToBytes();

        public static readonly byte[] NullBulkString = "$-1".ToBytes();
        public static readonly byte[] EmptyBulkString = "$0".ToBytes();

        public static readonly int CRLFLength = CRLF.Length;

        public static readonly CultureInfo InvariantCulture = CultureInfo.InvariantCulture;

        public static readonly byte[] Nil = "nil".ToBytes();

        public static readonly byte[] ZeroBytes = "0".ToBytes();

        // Commands that do not require DB
        public static readonly HashSet<RedisByteArray> CommandsForAnyRole = new HashSet<RedisByteArray> {
            "ROLE".ToBytes() ,
            "INFO".ToBytes() ,
            "PING".ToBytes() ,
            "CLIENT".ToBytes() ,
            "PSUBSCRIBE".ToBytes() ,
            "PUBLISH".ToBytes() ,
            "PUNSUBSCRIBE".ToBytes(),
            "QUIT".ToBytes(),
            "SLOWLOG".ToBytes(),
            "SUBSCRIBE".ToBytes(),
            "UNSUBSCRIBE".ToBytes()
        };

        public static readonly HashSet<RedisByteArray> CommandsNotRequireDB = new HashSet<RedisByteArray> {
            "AUTH".ToBytes(),
            "BGREWRITEAOF".ToBytes(),
            "BGSAVE".ToBytes(),
            "CLIENT".ToBytes(),
            "CLUSTER".ToBytes(),
            "SETNAME".ToBytes(),
            "CONFIG".ToBytes(),
            "DISCARD".ToBytes(),
            "ECHO".ToBytes(),
            "EXEC".ToBytes(),
            "FLUSHALL".ToBytes(),
            "INFO".ToBytes(),
            "LASTSAVE".ToBytes(),
            "MONITOR".ToBytes(),
            "MULTI".ToBytes(),
            "PING".ToBytes(),
            "PSUBSCRIBE".ToBytes(),
            "PUBLISH".ToBytes(),
            "PUNSUBSCRIBE".ToBytes(),
            "QUIT".ToBytes(),
            "SAVE".ToBytes(),
            "SCRIPT".ToBytes(),
            "SENTINEL".ToBytes(),
            "SLAVEOF".ToBytes(),
            "SLOWLOG".ToBytes(),
            "SUBSCRIBE".ToBytes(),
            "TIME".ToBytes(),
            "UNSUBSCRIBE".ToBytes(),
            "UNWATCH".ToBytes()        };

        public static readonly HashSet<RedisByteArray> CommandsThatUpdate = new HashSet<RedisByteArray> {
            "APPEND".ToBytes(),
            "BITOP".ToBytes(),
            "BLPOP".ToBytes(),
            "BRPOP".ToBytes(),
            "BRPOPLPUSH".ToBytes(),
            "DECR".ToBytes(),
            "DECRBY".ToBytes(),
            "DEL".ToBytes(),
            "EXPIRE".ToBytes(),
            "EXPIREAT".ToBytes(),
            "FLUSHALL".ToBytes(),
            "FLUSHDB".ToBytes(),
            "GETSET".ToBytes(),
            "HDEL".ToBytes(),
            "HINCRBY".ToBytes(),
            "HINCRBYFLOAT".ToBytes(),
            "HMSET".ToBytes(),
            "HSET".ToBytes(),
            "HSETNX".ToBytes(),
            "INCR".ToBytes(),
            "INCRBY".ToBytes(),
            "INCRBYFLOAT".ToBytes(),
            "LINSERT".ToBytes(),
            "LPOP".ToBytes(),
            "LPUSH".ToBytes(),
            "LPUSHX".ToBytes(),
            "LREM".ToBytes(),
            "LSET".ToBytes(),
            "LTRIM".ToBytes(),
            "MIGRATE".ToBytes(),
            "MOVE".ToBytes(),
            "MSET".ToBytes(),
            "MSETNX".ToBytes(),
            "PERSIST".ToBytes(),
            "PEXPIRE".ToBytes(),
            "PEXPIREAT".ToBytes(),
            "PFADD".ToBytes(),
            "PFMERGE".ToBytes(),
            "PSETEX".ToBytes(),
            "RENAME".ToBytes(),
            "RENAMENX".ToBytes(),
            "RESTORE".ToBytes(),
            "RPOP".ToBytes(),
            "RPOPLPUSH".ToBytes(),
            "RPUSH".ToBytes(),
            "RPUSHX".ToBytes(),
            "SADD".ToBytes(),
            "SDIFFSTORE".ToBytes(),
            "SET".ToBytes(),
            "SETBIT".ToBytes(),
            "SETEX".ToBytes(),
            "SETNX".ToBytes(),
            "SETRANGE".ToBytes(),
            "SINTERSTORE".ToBytes(),
            "SMOVE".ToBytes(),
            "SPOP".ToBytes(),
            "SREM".ToBytes(),
            "SUNIONSTORE".ToBytes(),
            "ZADD".ToBytes(),
            "ZINTERSTORE".ToBytes(),
            "ZINCRBY".ToBytes(),
            "ZREM".ToBytes(),
            "ZREMRANGEBYLEX".ToBytes(),
            "ZREMRANGEBYRANK".ToBytes(),
            "ZREMRANGEBYSCORE".ToBytes(),
            "ZUNIONSTORE".ToBytes()
        };

        #endregion Static Members

        #region Constants

        public const string SDownEntered = "+sdown";
        public const string SDownExited = "-sdown";
        public const string ODownEntered = "+odown";
        public const string ODownExited = "-odown";
        public const string SwitchMaster = "+switch-master";
        public const string SentinelDiscovered = "+sentinel";

        public const int RedisBatchBase = 1000;

        public const string Warning = "Warning";
        public const string FatalException = "Fatal exception";

        public const string OK = "OK";
        public const string QUEUED = "QUEUED";
        public const string PONG = "PONG";

        public const string SentinelHelloChannel = "__sentinel__:hello";

        public const string CRLF = "\r\n";

        public const int DefaultPort = 6379;
        public const int DefaultSentinelPort = 26379;

        public const string LocalHost = "localhost";
        public const string IP4Loopback = "127.0.0.1";
        public const string IP6Loopback = "::1";

        public const long Zero = 0L;
        public const long One = 1L;
        public const long MinusOne = -1L;

        public const long True = 1L;
        public const long False = 0L;

        public const int ReadBufferSize = 16 * 1024;
        public const int WriteBufferSize = 2 * 1024;

        public const int MaxValueLength = 1024 * 1024 * 1024; // 1 GB

        public const int ConnectionPurgePeriod = 1000; // milliseconds

        public const int MinDbIndex = -1;
        public const int MaxDbIndex = 16;

        public const int CardioProbeStatusChangeRetryCount = 3;

        public const int DefaultHeartBeatIntervalSecs = 3;
        public const int MinHeartBeatIntervalSecs = 2;
        public const int MaxHeartBeatIntervalSecs = 120;

        public const int DefaultConnectionTimeout = 10000;
        public const int MinConnectionTimeout = 100;
        public const int MaxConnectionTimeout = 60000;

        public const int MinConnectionCount = 1;
        public const int MaxConnectionCount = 1000;
        public const int DefaultMaxConnectionCount = 5;

        public const int DefaultWaitTimeout = 5000;
        public const int MinWaitTimeout = 1000;
        public const int MaxWaitTimeout = 30000;

        public const int DefaultWaitRetryCount = 3;
        public const int MinWaitRetryCount = 1;
        public const int MaxWaitRetryCount = 10;

        public const int DefaultIdleTimeout = 300000;
        public const int MinIdleTimeout = 10000;
        public const int MaxIdleTimeout = 1200000;

        public const int DefaultSendTimeout = 15000;
        public const int MinSendTimeout = 100;
        public const int MaxSendTimeout = 60000;

        public const int DefaultReceiveTimeout = 15000;
        public const int MinReceiveTimeout = 100;
        public const int MaxReceiveTimeout = 60000;

        #endregion Constants
    }
}
