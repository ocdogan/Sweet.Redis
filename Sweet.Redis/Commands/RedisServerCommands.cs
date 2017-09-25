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

        public RedisServerCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisBool BGRewriteAOF()
        {
            return ExpectSimpleString(RedisCommands.BGRewriteAOF, RedisConstants.OK);
        }

        public RedisBool BGSave()
        {
            return ExpectSimpleString(RedisCommands.BGSave, RedisConstants.OK);
        }

        public RedisString ClientGetName()
        {
            return ExpectBulkString(RedisCommands.Client, RedisCommands.GetName);
        }

        public RedisInt ClientKill(string ip = null, int port = -1, string clientId = null, string type = null, bool skipMe = true)
        {
            ValidateNotDisposed();

            var parameters = new byte[0].Join(RedisCommands.Kill);

            if (!String.IsNullOrEmpty(ip))
            {
                parameters = parameters
                    .Join(RedisCommands.Addr)
                    .Join(ip.ToBytes());

                if (port > -1)
                    parameters = parameters.Join(port.ToBytes());
            }

            if (!String.IsNullOrEmpty(clientId))
            {
                parameters = parameters
                    .Join(RedisCommands.Id)
                    .Join(clientId.ToBytes());
            }

            if (!String.IsNullOrEmpty(type))
            {
                parameters = parameters
                    .Join(RedisCommands.Type)
                    .Join(type.ToBytes());
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
            if (response != null)
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
            if (response != null)
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
            return ExpectSimpleString(RedisCommands.Client, RedisConstants.OK, RedisCommands.Pause, timeout.ToBytes());
        }

        public RedisBool ClientReplyOff()
        {
            return ExpectSimpleString(RedisCommands.Client, RedisConstants.OK, RedisCommands.Reply, RedisCommands.Off);
        }

        public RedisBool ClientReplyOn()
        {
            return ExpectSimpleString(RedisCommands.Client, RedisConstants.OK, RedisCommands.Reply, RedisCommands.On);
        }

        public RedisBool ClientReplySkip()
        {
            return ExpectSimpleString(RedisCommands.Client, RedisConstants.OK, RedisCommands.Reply, RedisCommands.Skip);
        }

        public RedisBool ClientSetName(string connectionName)
        {
            if (connectionName == null)
                throw new ArgumentNullException("connectionName");

            return ExpectSimpleString(RedisCommands.Client, RedisConstants.OK, RedisCommands.SetName, connectionName.ToBytes());
        }

        public RedisResult<IDictionary<string, string>> ConfigGet(string parameter)
        {
            if (parameter == null)
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
            return ExpectSimpleString(RedisCommands.Config, RedisConstants.OK, RedisCommands.ResetStat);
        }

        public RedisBool ConfigRewrite()
        {
            return ExpectSimpleString(RedisCommands.Config, RedisConstants.OK, RedisCommands.Rewrite);
        }

        public RedisBool ConfigSet(string parameter, string value)
        {
            if (parameter == null)
                throw new ArgumentNullException("parameter");

            if (value == null)
                throw new ArgumentNullException("value");

            ValidateNotDisposed();

            var bytes = value.ToBytes();
            if (bytes != null && bytes.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("value is limited to 1GB", "value");

            return ExpectSimpleString(RedisCommands.Config, RedisConstants.OK, RedisCommands.Set, parameter.ToBytes(), bytes);
        }

        public RedisInt DbSize()
        {
            return ExpectInteger(RedisCommands.DbSize);
        }

        public RedisBool FlushAll()
        {
            return ExpectSimpleString(RedisCommands.FlushAll, RedisConstants.OK);
        }

        public RedisBool FlushAllAsync()
        {
            return ExpectSimpleString(RedisCommands.FlushAll, RedisConstants.OK, RedisCommands.Async);
        }

        public RedisBool FlushDb()
        {
            return ExpectSimpleString(RedisCommands.FlushDb, RedisConstants.OK);
        }

        public RedisBool FlushDbAsync()
        {
            return ExpectSimpleString(RedisCommands.FlushDb, RedisConstants.OK, RedisCommands.Async);
        }

        public RedisMultiString Info(string section)
        {
            if (section == null)
                throw new ArgumentNullException("section");

            return ExpectMultiDataStrings(RedisCommands.Info, section.ToBytes());
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

        public RedisBool Save()
        {
            return ExpectSimpleString(RedisCommands.Save, RedisConstants.OK);
        }

        public RedisVoid ShutDown()
        {
            ExpectSimpleString(RedisCommands.ShutDown);
            return new RedisVoid();
        }

        public RedisVoid ShutDownSave()
        {
            ExpectSimpleString(RedisCommands.ShutDown, RedisCommands.Async);
            return new RedisVoid();
        }

        public RedisBool SlaveOf(string host, int port)
        {
            if (host == null)
                throw new ArgumentNullException("host");

            if (port < 0 || port > ushort.MaxValue)
                throw new ArgumentException("Invalid port number");

            return ExpectSimpleString(RedisCommands.SlaveOf, RedisConstants.OK, host.ToBytes(), port.ToBytes());
        }

        public RedisBool SlaveOfNoOne()
        {
            return ExpectSimpleString(RedisCommands.SlaveOf, RedisConstants.OK, RedisCommands.NoOne);
        }

        public RedisResult<RedisSlowLogInfo[]> SlowLogGet(int count)
        {
            var response = ExpectArray(RedisCommands.SlowLog, RedisCommands.Get, count.ToBytes());
            if (response != null)
                return new RedisResult<RedisSlowLogInfo[]>(RedisSlowLogInfo.ToSlowLogInfo(response.Value));
            return new RedisResult<RedisSlowLogInfo[]>(null);
        }

        public RedisInt SlowLogLen()
        {
            return ExpectInteger(RedisCommands.SlowLog, RedisCommands.Len);
        }

        public RedisBool SlowLogReset()
        {
            return ExpectSimpleString(RedisCommands.SlowLog, RedisConstants.OK, RedisCommands.Reset);
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
