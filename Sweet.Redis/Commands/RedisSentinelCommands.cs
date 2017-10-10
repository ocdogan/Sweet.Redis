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
    internal class RedisSentinelCommands : RedisCommandSet, IRedisSentinelCommands
    {
        #region .Ctors

        public RedisSentinelCommands(RedisSentinelClient client)
            : base(client)
        { }

        #endregion .Ctors

        #region Methods

        public RedisBool Ping()
        {
            return ExpectSimpleString(RedisCommands.Ping, RedisConstants.OK);
        }

        public RedisString CheckQuorum(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            return ExpectSimpleString(RedisCommands.Sentinel, RedisCommands.SentinelCheckQuorum);
        }

        public RedisBool Failover(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            return ExpectSimpleString(RedisCommands.Sentinel, RedisConstants.OK, RedisCommands.SentinelFailover, masterName.ToBytes());
        }

        public RedisBool FlushConfig()
        {
            return ExpectSimpleString(RedisCommands.Sentinel, RedisConstants.OK, RedisCommands.SentinelFlushConfig);
        }

        public RedisResult<RedisEndPoint> GetMasterAddrByName(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            var raw = ExpectArray(RedisCommands.Sentinel, RedisCommands.SentinelGetMasterAddrByName, masterName.ToBytes());
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var list = rawValue.Items;
                    if (list != null)
                    {
                        var count = list.Count;
                        if (count > 0)
                        {
                            var rawIP = list[0];
                            if (!ReferenceEquals(rawIP, null) && rawIP.Type == RedisRawObjectType.BulkString)
                            {
                                var ipAddress = rawIP.Data as string;
                                if (!String.IsNullOrEmpty(ipAddress))
                                {
                                    int port = 0;
                                    if (count > 1)
                                    {
                                        var rawPort = list[0];
                                        if (!ReferenceEquals(rawPort, null) && rawPort.Type == RedisRawObjectType.Integer)
                                        {
                                            var data = rawPort.Data;
                                            if (data is long)
                                                port = (int)(long)data;
                                            else if (data is int)
                                                port = (int)data;
                                        }
                                    }

                                    return new RedisResult<RedisEndPoint>(new RedisEndPoint(ipAddress, port));
                                }
                            }
                        }
                    }
                }
            }
            return new RedisResult<RedisEndPoint>(null);
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

        public RedisBool IsMasterDownByAddr(string ipAddress, int port)
        {
            if (String.IsNullOrEmpty(ipAddress))
                throw new ArgumentNullException("ipAddress");

            return ExpectSimpleString(RedisCommands.Sentinel, RedisConstants.OK, RedisCommands.SentinelIsMasterDownByAddr);
        }

        public RedisResult<RedisSentinelMasterInfo> Master(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            var raw = ExpectArray(RedisCommands.Sentinel, RedisCommands.SentinelMaster, masterName.ToBytes());
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var info = new RedisSentinelMasterInfo(rawValue);
                    return new RedisResult<RedisSentinelMasterInfo>(info);
                }
            }
            return new RedisResult<RedisSentinelMasterInfo>(null);
        }

        public RedisResult<RedisSentinelMasterInfo[]> Masters()
        {
            var raw = ExpectArray(RedisCommands.Sentinel, RedisCommands.SentinelMasters);
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var list = rawValue.Items;
                    if (list != null)
                    {
                        var count = list.Count;
                        if (count > 0)
                        {
                            var items = new List<RedisSentinelMasterInfo>(count);
                            for (var i = 0; i < count; i++)
                            {
                                var item = list[i];
                                if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Array)
                                {
                                    var info = new RedisSentinelMasterInfo(rawValue);
                                    items.Add(info);
                                }
                            }

                            if (items.Count > 0)
                                return new RedisResult<RedisSentinelMasterInfo[]>(items.ToArray());
                        }
                    }
                }
            }
            return new RedisResult<RedisSentinelMasterInfo[]>(null);
        }

        public RedisBool Monitor(string masterName, string ipAddress, int port, int quorum)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            if (String.IsNullOrEmpty(ipAddress))
                throw new ArgumentNullException("ipAddress");

            throw new System.NotImplementedException();
        }

        public RedisBool Remove(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            throw new System.NotImplementedException();
        }

        public RedisInteger Reset(RedisParam pattern)
        {
            if (pattern.IsNull)
                throw new ArgumentNullException("pattern");

            return ExpectInteger(RedisCommands.Sentinel, RedisCommands.SentinelReset, pattern);
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

        public RedisResult<RedisSentinelNodeInfo[]> Sentinels(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            var raw = ExpectArray(RedisCommands.Sentinel, RedisCommands.Sentinels);
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var list = rawValue.Items;
                    if (list != null)
                    {
                        var count = list.Count;
                        if (count > 0)
                        {
                            var items = new List<RedisSentinelNodeInfo>(count);
                            for (var i = 0; i < count; i++)
                            {
                                var item = list[i];
                                if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Array)
                                {
                                    var info = new RedisSentinelNodeInfo(rawValue);
                                    items.Add(info);
                                }
                            }

                            if (items.Count > 0)
                                return new RedisResult<RedisSentinelNodeInfo[]>(items.ToArray());
                        }
                    }
                }
            }
            return new RedisResult<RedisSentinelNodeInfo[]>(null);
        }

        public RedisBool Set(string masterName, RedisParam parameter, RedisParam value)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            if (parameter.IsNull)
                throw new ArgumentNullException("parameter");

            if (value.IsNull)
                throw new ArgumentNullException("value");

            return ExpectSimpleString(RedisCommands.Sentinel, RedisConstants.OK, RedisCommands.SentinelSet, parameter, value);
        }

        public RedisResult<RedisSentinelSlaveInfo[]> Slaves(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            var raw = ExpectArray(RedisCommands.Sentinel, RedisCommands.SentinelSlaves);
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var list = rawValue.Items;
                    if (list != null)
                    {
                        var count = list.Count;
                        if (count > 0)
                        {
                            var items = new List<RedisSentinelSlaveInfo>(count);
                            for (var i = 0; i < count; i++)
                            {
                                var item = list[i];
                                if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Array)
                                {
                                    var info = new RedisSentinelSlaveInfo(rawValue);
                                    items.Add(info);
                                }
                            }

                            if (items.Count > 0)
                                return new RedisResult<RedisSentinelSlaveInfo[]>(items.ToArray());
                        }
                    }
                }
            }
            return new RedisResult<RedisSentinelSlaveInfo[]>(null);
        }

        #endregion Methods
    }
}
