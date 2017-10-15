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
using System.Collections.Generic;

namespace Sweet.Redis
{
    internal class RedisServerCommands : RedisCommandSet, IRedisServerCommands
    {
        #region .Ctors

        public RedisServerCommands(RedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisBool BGRewriteAOF()
        {
            return ExpectOK(RedisCommands.BGRewriteAOF);
        }

        public RedisBool BGSave()
        {
            return ExpectOK(RedisCommands.BGSave);
        }

        public RedisString ClientGetName()
        {
            return ExpectBulkString(RedisCommands.Client, RedisCommands.GetName);
        }

        public RedisInteger ClientKill(RedisParam? ip = null, int port = -1, RedisParam? clientId = null, RedisParam? type = null, bool skipMe = true)
        {
            ValidateNotDisposed();

            var parameters = new byte[0].Join(RedisCommands.Kill);

            if (ip.HasValue && !ip.Value.IsEmpty)
            {
                parameters = parameters
                    .Join(RedisCommands.Addr)
                    .Join(ip.Value);

                if (port > -1)
                    parameters = parameters.Join(port.ToBytes());
            }

            if (clientId.HasValue && !clientId.Value.IsEmpty)
            {
                parameters = parameters
                    .Join(RedisCommands.Id)
                    .Join(clientId);
            }

            if (type.HasValue && !type.Value.IsEmpty)
            {
                parameters = parameters
                    .Join(RedisCommands.Type)
                    .Join(type);
            }

            if (!skipMe)
            {
                parameters = parameters
                    .Join(RedisCommands.SkipMe)
                    .Join(RedisCommands.No);
            }

            return ExpectInteger(RedisCommands.Client, parameters);
        }

        public RedisResult<RedisClientInfo[]> ClientList()
        {
            var response = ExpectBulkString(RedisCommands.Client, RedisCommands.List);
            if (response != (string)null)
            {
                var value = response.Value;
                if (!String.IsNullOrEmpty(value))
                {
                    var lines = value.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    if (lines.Length > 0)
                    {
                        var list = new List<RedisClientInfo>(lines.Length);
                        foreach (var line in lines)
                        {
                            var info = RedisClientInfo.Parse(line);
                            if (info != null)
                                list.Add(info);
                        }
                        return new RedisResult<RedisClientInfo[]>((list.Count > 0) ? list.ToArray() : null);
                    }
                }
            }
            return new RedisResult<RedisClientInfo[]>(null);
        }

        public RedisResult<IDictionary<string, string>[]> ClientListDictionary()
        {
            var response = ExpectBulkString(RedisCommands.Client, RedisCommands.List);
            if (response != (string)null)
            {
                var value = response.Value;
                if (!String.IsNullOrEmpty(value))
                {
                    var lines = value.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    if (lines.Length > 0)
                    {
                        var list = new List<IDictionary<string, string>>(lines.Length);
                        foreach (var line in lines)
                        {
                            var info = RedisClientInfo.ParseDictionary(line);
                            if (info != null)
                                list.Add(info);
                        }
                        return new RedisResult<IDictionary<string, string>[]>((list.Count > 0) ? list.ToArray() : null);
                    }
                }
            }
            return new RedisResult<IDictionary<string, string>[]>(null);
        }

        public RedisBool ClientPause(int timeout)
        {
            return ExpectOK(RedisCommands.Client, RedisCommands.Pause, timeout.ToBytes());
        }

        public RedisBool ClientReplyOff()
        {
            return ExpectOK(RedisCommands.Client, RedisCommands.Reply, RedisCommands.Off);
        }

        public RedisBool ClientReplyOn()
        {
            return ExpectOK(RedisCommands.Client, RedisCommands.Reply, RedisCommands.On);
        }

        public RedisBool ClientReplySkip()
        {
            return ExpectOK(RedisCommands.Client, RedisCommands.Reply, RedisCommands.Skip);
        }

        public RedisBool ClientSetName(RedisParam connectionName)
        {
            if (connectionName.IsNull)
                throw new ArgumentNullException("connectionName");

            return ExpectOK(RedisCommands.Client, RedisCommands.SetName, connectionName.ToBytes());
        }

        public RedisResult<IDictionary<string, string>> ConfigGet(RedisParam parameter)
        {
            if (parameter.IsNull)
                throw new ArgumentNullException("parameter");

            var lines = ExpectMultiDataStrings(RedisCommands.Config, RedisCommands.Get, parameter.ToBytes());
            if (lines != null)
            {
                var length = lines.Length;
                if (lines.Length > 0)
                {
                    var result = new Dictionary<string, string>(lines.Length / 2);
                    for (var i = 0; i < length; i += 2)
                    {
                        var key = (lines[i] ?? String.Empty).Trim();
                        if (!String.IsNullOrEmpty(key))
                            result[key] = (lines[i + 1] ?? String.Empty).Trim();
                    }
                    return new RedisResult<IDictionary<string, string>>(result);
                }
            }
            return new RedisResult<IDictionary<string, string>>(null);
        }

        public RedisBool ConfigResetStat()
        {
            return ExpectOK(RedisCommands.Config, RedisCommands.ResetStat);
        }

        public RedisBool ConfigRewrite()
        {
            return ExpectOK(RedisCommands.Config, RedisCommands.Rewrite);
        }

        public RedisBool ConfigSet(RedisParam parameter, RedisParam value)
        {
            if (parameter.IsNull)
                throw new ArgumentNullException("parameter");

            if (value.IsNull)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            if (value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectOK(RedisCommands.Config, RedisCommands.Set, parameter, value);
        }

        public RedisInteger DbSize()
        {
            return ExpectInteger(RedisCommands.DbSize);
        }

        public RedisBool FlushAll()
        {
            return ExpectOK(RedisCommands.FlushAll);
        }

        public RedisBool FlushAllAsync()
        {
            return ExpectOK(RedisCommands.FlushAll, RedisCommands.Async);
        }

        public RedisBool FlushDb()
        {
            return ExpectOK(RedisCommands.FlushDb);
        }

        public RedisBool FlushDbAsync()
        {
            return ExpectOK(RedisCommands.FlushDb, RedisCommands.Async);
        }

        public RedisResult<RedisServerInfo> Info(RedisParam? section = null)
        {
            string lines;
            if (!section.HasValue || section.Value.IsNull)
                lines = ExpectBulkString(RedisCommands.Info);
            else
                lines = ExpectBulkString(RedisCommands.Info, section);

            var info = RedisServerInfo.Parse(lines);
            return new RedisResult<RedisServerInfo>(info);
        }

        public RedisDate LastSave()
        {
            var response = ExpectInteger(RedisCommands.LastSave);
            if (response != null)
            {
                return response.Value.FromUnixTimeStamp();
            }
            return DateTime.MinValue;
        }

        public RedisResult<RedisRoleInfo> Role()
        {
            var raw = ExpectArray(RedisCommands.Role);
            if (!ReferenceEquals(raw, null))
            {
                var result = RedisRoleInfo.Parse(raw.Value);
                return new RedisResult<RedisRoleInfo>(result);
            }
            return new RedisResult<RedisRoleInfo>(null);
        }

        public RedisBool Save()
        {
            return ExpectOK(RedisCommands.Save);
        }

        public RedisVoid ShutDown()
        {
            ExpectNothing(RedisCommands.ShutDown);
            return new RedisVoid();
        }

        public RedisVoid ShutDownSave()
        {
            ExpectNothing(RedisCommands.ShutDown, RedisCommands.Async);
            return new RedisVoid();
        }

        public RedisBool SlaveOf(RedisParam host, int port)
        {
            if (host.IsNull)
                throw new ArgumentNullException("host");

            if (port < 0 || port > ushort.MaxValue)
                throw new ArgumentException("Invalid port number");

            return ExpectOK(RedisCommands.SlaveOf, host, port.ToBytes());
        }

        public RedisBool SlaveOfNoOne()
        {
            return ExpectOK(RedisCommands.SlaveOf, RedisCommands.NoOne);
        }

        public RedisResult<RedisSlowLogInfo[]> SlowLogGet(int count)
        {
            var response = ExpectArray(RedisCommands.SlowLog, RedisCommands.Get, count.ToBytes());
            if (response != null)
                return new RedisResult<RedisSlowLogInfo[]>(RedisSlowLogInfo.ToSlowLogInfo(response.Value));
            return new RedisResult<RedisSlowLogInfo[]>(null);
        }

        public RedisInteger SlowLogLen()
        {
            return ExpectInteger(RedisCommands.SlowLog, RedisCommands.Len);
        }

        public RedisBool SlowLogReset()
        {
            return ExpectOK(RedisCommands.SlowLog, RedisCommands.Reset);
        }

        public RedisDate Time()
        {
            var parts = ExpectMultiDataStrings(RedisCommands.Time);
            if (parts != null && parts.Length > 0)
            {
                if (parts.Length > 1)
                    return parts[0].ToInt().FromUnixTimeStamp(parts[1].ToInt());
                return parts[0].ToInt().FromUnixTimeStamp();
            }
            return DateTime.MinValue;
        }

        #endregion Methods
    }
}
