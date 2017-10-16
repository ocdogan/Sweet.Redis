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
        public static readonly Dictionary<byte[], bool> CommandsNotRequireDB = new Dictionary<byte[], bool> {
            { "AUTH".ToBytes(), true },
            { "BGREWRITEAOF".ToBytes(), true },
            { "BGSAVE".ToBytes(), true },
            { "CLIENT".ToBytes(), true },
            { "CLUSTER".ToBytes(), true },
            { "SETNAME".ToBytes(), true },
            { "CONFIG".ToBytes(), true },
            { "DISCARD".ToBytes(), true },
            { "ECHO".ToBytes(), true },
            { "EXEC".ToBytes(), true },
            { "FLUSHALL".ToBytes(), true },
            { "INFO".ToBytes(), true },
            { "LASTSAVE".ToBytes(), true },
            { "MONITOR".ToBytes(), true },
            { "MULTI".ToBytes(), true },
            { "PING".ToBytes(), true },
            { "PSUBSCRIBE".ToBytes(), true },
            { "PUBLISH".ToBytes(), true },
            { "PUNSUBSCRIBE".ToBytes(), true },
            { "QUIT".ToBytes(), true },
            { "SAVE".ToBytes(), true },
            { "SCRIPT".ToBytes(), true },
            { "SENTINEL".ToBytes(), true },
            { "SLAVEOF".ToBytes(), true },
            { "SLOWLOG".ToBytes(), true },
            { "SUBSCRIBE".ToBytes(), true },
            { "TIME".ToBytes(), true },
            { "UNSUBSCRIBE".ToBytes(), true },
            { "UNWATCH".ToBytes(), true }
        };

        public static readonly Dictionary<byte[], bool> CommandsThatUpdate = new Dictionary<byte[], bool> {
            { "APPEND".ToBytes(), true },
            { "BITOP".ToBytes(), true },
            { "BLPOP".ToBytes(), true },
            { "BRPOP".ToBytes(), true },
            { "BRPOPLPUSH".ToBytes(), true },
            { "DECR".ToBytes(), true },
            { "DECRBY".ToBytes(), true },
            { "DEL".ToBytes(), true },
            { "EXPIRE".ToBytes(), true },
            { "EXPIREAT".ToBytes(), true },
            { "FLUSHALL".ToBytes(), true },
            { "FLUSHDB".ToBytes(), true },
            { "GETSET".ToBytes(), true },
            { "HDEL".ToBytes(), true },
            { "HINCRBY".ToBytes(), true },
            { "HINCRBYFLOAT".ToBytes(), true },
            { "HMSET".ToBytes(), true },
            { "HSET".ToBytes(), true },
            { "HSETNX".ToBytes(), true },
            { "INCR".ToBytes(), true },
            { "INCRBY".ToBytes(), true },
            { "INCRBYFLOAT".ToBytes(), true },
            { "LINSERT".ToBytes(), true },
            { "LPOP".ToBytes(), true },
            { "LPUSH".ToBytes(), true },
            { "LPUSHX".ToBytes(), true },
            { "LREM".ToBytes(), true },
            { "LSET".ToBytes(), true },
            { "LTRIM".ToBytes(), true },
            { "MIGRATE".ToBytes(), true },
            { "MOVE".ToBytes(), true },
            { "MSET".ToBytes(), true },
            { "MSETNX".ToBytes(), true },
            { "PERSIST".ToBytes(), true },
            { "PEXPIRE".ToBytes(), true },
            { "PEXPIREAT".ToBytes(), true },
            { "PFADD".ToBytes(), true },
            { "PFMERGE".ToBytes(), true },
            { "PSETEX".ToBytes(), true },
            { "RENAME".ToBytes(), true },
            { "RENAMENX".ToBytes(), true },
            { "RESTORE".ToBytes(), true },
            { "RPOP".ToBytes(), true },
            { "RPOPLPUSH".ToBytes(), true },
            { "RPUSH".ToBytes(), true },
            { "RPUSHX".ToBytes(), true },
            { "SADD".ToBytes(), true },
            { "SDIFFSTORE".ToBytes(), true },
            { "SET".ToBytes(), true },
            { "SETBIT".ToBytes(), true },
            { "SETEX".ToBytes(), true },
            { "SETNX".ToBytes(), true },
            { "SETRANGE".ToBytes(), true },
            { "SINTERSTORE".ToBytes(), true },
            { "SMOVE".ToBytes(), true },
            { "SPOP".ToBytes(), true },
            { "SREM".ToBytes(), true },
            { "SUNIONSTORE".ToBytes(), true },
            { "ZADD".ToBytes(), true },
            { "ZINTERSTORE".ToBytes(), true },
            { "ZINCRBY".ToBytes(), true },
            { "ZREM".ToBytes(), true },
            { "ZREMRANGEBYLEX".ToBytes(), true },
            { "ZREMRANGEBYRANK".ToBytes(), true },
            { "ZREMRANGEBYSCORE".ToBytes(), true },
            { "ZUNIONSTORE".ToBytes(), true },
        };

        #endregion Static Members

        #region Constants

        public const int RedisBatchBase = 1000;

        public const string FatalException = "Fatal exception";

        public const string OK = "OK";
        public const string QUEUED = "QUEUED";
        public const string PONG = "PONG";

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
