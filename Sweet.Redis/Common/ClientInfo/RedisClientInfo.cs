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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Sweet.Redis
{
    public class RedisClientInfo
    {
        #region Static Members

        private static readonly Dictionary<char, RedisClientInfoFlag> FlagDefs =
            new Dictionary<char, RedisClientInfoFlag>
            {
                { 'N', RedisClientInfoFlag.None }, // no specific flag set
                { 'O', RedisClientInfoFlag.MonitoringSlave }, // the client is a slave in MONITOR mode
                { 'S', RedisClientInfoFlag.Slave }, // the client is a normal slave server
                { 'M', RedisClientInfoFlag.Master }, // the client is a master
                { 'x', RedisClientInfoFlag.MutiExecContext }, // the client is in a MULTI/EXEC context
                { 'b', RedisClientInfoFlag.WaitingBlockingOp }, // the client is waiting in a blocking operation
                { 'i', RedisClientInfoFlag.WaitingIO }, // the client is waiting for a VM I/O (deprecated)
                { 'd', RedisClientInfoFlag.WatchedKeysModified }, // a watched keys has been modified - EXEC will fail
                { 'c', RedisClientInfoFlag.ClosingAfterReply }, // connection to be closed after writing entire reply
                { 'u', RedisClientInfoFlag.Unblocked }, // the client is unblocked
                { 'U', RedisClientInfoFlag.UnixSocket }, // the client is connected via a Unix domain socket
                { 'r', RedisClientInfoFlag.ReadOnlyClusterNode }, // the client is in readonly mode against a cluster node
                { 'A', RedisClientInfoFlag.ClosingASAP }  // connection to be closed ASAP
			};

        #endregion Static Members

        #region .Ctors

        private RedisClientInfo(long id, string address, string socketFileDescriptor, long ageSeconds,
            long idleSeconds, RedisClientInfoFlag flags, int db, int subsCount, int psubsCount, int multiCount,
            int queryBufLen, int queryBufFreeLen, int outputBufLen, int outputListLen, int outputBufMemUsage,
            string fileDescriptorEvents, string lastCommand)
        {
            Id = id;
            Address = address;
            SocketFileDescriptor = socketFileDescriptor;
            AgeSeconds = ageSeconds;
            IdleSeconds = idleSeconds;
            Flags = flags;
            Db = db;
            SubsCount = subsCount;
            PSubsCount = psubsCount;
            MultiCount = multiCount;
            QueryBufLen = queryBufLen;
            QueryBufFreeLen = queryBufFreeLen;
            OutputBufLen = outputBufLen;
            OutputListLen = outputListLen;
            OutputBufMemUsage = outputBufMemUsage;
            FileDescriptorEvents = fileDescriptorEvents;
            LastCommand = lastCommand;
        }

        #endregion .Ctors

        #region Properties

        public long Id { get; private set; }

        public string Address { get; private set; }

        public string SocketFileDescriptor { get; private set; }

        public long AgeSeconds { get; private set; }

        public long IdleSeconds { get; private set; }

        public RedisClientInfoFlag Flags { get; private set; }

        public int Db { get; private set; }

        public int SubsCount { get; private set; }

        public int PSubsCount { get; private set; }

        public int MultiCount { get; private set; }

        public int QueryBufLen { get; private set; }

        public int QueryBufFreeLen { get; private set; }

        public int OutputBufLen { get; private set; }

        public int OutputListLen { get; private set; }

        public int OutputBufMemUsage { get; private set; }

        public string FileDescriptorEvents { get; private set; }

        public string LastCommand { get; private set; }

        #endregion Properties

        #region Methods

        public static IDictionary<string, string> ParseDictionary(string line)
        {
            if (line != null)
            {
                var items = line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(s =>
                    {
                        var key = s;
                        var value = String.Empty;

                        var pos = s.IndexOf('=');
                        if (pos > -1)
                        {
                            key = s.Substring(0, pos);
                            value = s.Substring(pos + 1, s.Length - pos - 1);
                        }

                        return new
                        {
                            Key = (key ?? String.Empty).ToLowerInvariant(),
                            Value = value
                        };
                    });

                var result = new Dictionary<string, string>();
                foreach (var item in items)
                    result[item.Key] = item.Value;

                return result;
            }
            return null;
        }

        public static RedisClientInfo Parse(string line)
        {
            if (line != null)
            {
                var items = line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(s =>
                    {
                        var key = s;
                        var value = String.Empty;

                        var pos = s.IndexOf('=');
                        if (pos > -1)
                        {
                            key = s.Substring(0, pos);
                            value = s.Substring(pos + 1, s.Length - pos - 1);
                        }

                        return new
                        {
                            Key = (key ?? String.Empty).ToLowerInvariant(),
                            Value = value
                        };
                    });

                var ht = new Hashtable();
                foreach (var item in items)
                    ht[item.Key] = item.Value;

                /*
                id: an unique 64-bit client ID (introduced in Redis 2.8.12).
                addr: address/port of the client
                fd: file descriptor corresponding to the socket
                age: total duration of the connection in seconds
                idle: idle time of the connection in seconds
                flags: client flags (see below)
                db: current database ID
                sub: number of channel subscriptions
                psub: number of pattern matching subscriptions
                multi: number of commands in a MULTI/EXEC context
                qbuf: query buffer length (0 means no query pending)
                qbuf-free: free space of the query buffer (0 means the buffer is full)
                obl: output buffer length
                oll: output list length (replies are queued in this list when the buffer is full)
                omem: output buffer memory usage
                events: file descriptor events (see below)
                cmd: last command played
                */

                var id = ((string)ht["id"]).ToLong(-1L);
                var address = (string)ht["addr"];
                var socketFileDescriptor = (string)ht["fd"];
                var ageSeconds = ((string)ht["age"]).ToLong(-1L);
                var idleSeconds = ((string)ht["idle"]).ToLong(-1L);
                var flags = ParseFlags((string)ht["flags"]);
                var db = ((string)ht["db"]).ToInt(-1);
                var subsCount = ((string)ht["sub"]).ToInt(-1);
                var psubsCount = ((string)ht["psub"]).ToInt(-1);
                var multiCount = ((string)ht["multi"]).ToInt(-1);
                var queryBufLen = ((string)ht["qbuf"]).ToInt(-1);
                var queryBufFreeLen = ((string)ht["qbuf-free"]).ToInt(-1);
                var outputBufLen = ((string)ht["obl"]).ToInt(-1);
                var outputListLen = ((string)ht["oll"]).ToInt(-1);
                var outputBufMemUsage = ((string)ht["omem"]).ToInt(-1);
                var fileDescriptorEvents = (string)ht["events"];
                var lastCommand = (string)ht["cmd"];

                return new RedisClientInfo(id, address, socketFileDescriptor, ageSeconds,
                     idleSeconds, flags, db, subsCount, psubsCount, multiCount, queryBufLen,
                     queryBufFreeLen, outputBufLen, outputListLen, outputBufMemUsage,
                     fileDescriptorEvents, lastCommand);
            }
            return null;
        }

        private static RedisClientInfoFlag ParseFlags(string flags)
        {
            var result = RedisClientInfoFlag.None;
            if (!flags.IsEmpty())
            {
                RedisClientInfoFlag flag;
                foreach (var ch in flags)
                    if (FlagDefs.TryGetValue(ch, out flag))
                        result |= flag;
            }
            return result;
        }

        #endregion Methods
    }
}
