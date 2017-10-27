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

        public RedisString CheckQuorum(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            return ExpectSimpleString(RedisCommandList.Sentinel, RedisCommandList.SentinelCheckQuorum);
        }

        public RedisBool Failover(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            return ExpectOK(RedisCommandList.Sentinel, RedisCommandList.SentinelFailover, masterName.ToBytes());
        }

        public RedisBool FlushConfig()
        {
            return ExpectOK(RedisCommandList.Sentinel, RedisCommandList.SentinelFlushConfig);
        }

        public RedisResult<RedisEndPointInfo> GetMasterAddrByName(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            var raw = ExpectArray(RedisCommandList.Sentinel, RedisCommandList.SentinelGetMasterAddrByName, masterName.ToBytes());
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var items = rawValue.Items;
                    if (items != null)
                    {
                        var count = items.Count;
                        if (count > 0)
                        {
                            var rawIP = items[0];
                            if (!ReferenceEquals(rawIP, null) && rawIP.Type == RedisRawObjectType.BulkString)
                            {
                                var ipAddress = rawIP.DataText;
                                if (!String.IsNullOrEmpty(ipAddress))
                                {
                                    int port = 0;
                                    if (count > 1)
                                    {
                                        var rawPort = items[1];
                                        if (!ReferenceEquals(rawPort, null) && rawPort.Type == RedisRawObjectType.BulkString)
                                        {
                                            var data = rawPort.DataText;
                                            if (!String.IsNullOrEmpty(data))
                                            {
                                                long l;
                                                if (long.TryParse(data, out l))
                                                    port = (int)l;
                                            }
                                        }
                                    }

                                    return new RedisResult<RedisEndPointInfo>(new RedisEndPointInfo(ipAddress, port));
                                }
                            }
                        }
                    }
                }
            }
            return new RedisResult<RedisEndPointInfo>(null);
        }

        public RedisResult<RedisServerInfo> Info(RedisParam? section = null)
        {
            string lines;
            if (!section.HasValue || section.Value.IsNull)
                lines = ExpectBulkString(RedisCommandList.Info);
            else
                lines = ExpectBulkString(RedisCommandList.Info, section);

            var info = RedisServerInfo.Parse(lines);
            return new RedisResult<RedisServerInfo>(info);
        }

        public RedisResult<RedisIsMasterDownInfo> IsMasterDownByAddr(string ipAddress, int port, string runId)
        {
            if (String.IsNullOrEmpty(ipAddress))
                throw new ArgumentNullException("ipAddress");

            if (String.IsNullOrEmpty(runId))
                throw new ArgumentNullException("runId");

            var raw = ExpectArray(RedisCommandList.Sentinel, RedisCommandList.SentinelIsMasterDownByAddr,
                                  ipAddress.ToBytes(), port.ToBytes(),
                                  ((long)RedisCommon.EpochNow()).ToBytes(), runId.ToBytes());
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var info = RedisIsMasterDownInfo.Parse(rawValue);
                    return new RedisResult<RedisIsMasterDownInfo>(info);
                }
            }
            return new RedisResult<RedisIsMasterDownInfo>(RedisIsMasterDownInfo.Empty);
        }

        public RedisResult<RedisSentinelMasterInfo> Master(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            var raw = ExpectArray(RedisCommandList.Sentinel, RedisCommandList.SentinelMaster, masterName.ToBytes());
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
            var raw = ExpectArray(RedisCommandList.Sentinel, RedisCommandList.SentinelMasters);
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var items = rawValue.Items;
                    if (items != null)
                    {
                        var count = items.Count;
                        if (count > 0)
                        {
                            var list = new List<RedisSentinelMasterInfo>(count);
                            for (var i = 0; i < count; i++)
                            {
                                var item = items[i];
                                if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Array)
                                {
                                    var info = new RedisSentinelMasterInfo(item);
                                    list.Add(info);
                                }
                            }

                            if (list.Count > 0)
                                return new RedisResult<RedisSentinelMasterInfo[]>(list.ToArray());
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

            return ExpectOK(RedisCommandList.Sentinel, RedisCommandList.SentinelMonitor,
                                      masterName.ToBytes(), ipAddress.ToBytes(), port.ToBytes(), quorum.ToBytes());
        }

        public RedisBool Ping()
        {
            var pong = ExpectSimpleString(RedisCommandList.Ping);
            return pong == RedisConstants.PONG;
        }

        public RedisBool Remove(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            return ExpectOK(RedisCommandList.Sentinel, RedisCommandList.SentinelRemove, masterName.ToBytes());
        }

        public RedisInteger Reset(RedisParam pattern)
        {
            if (pattern.IsNull)
                throw new ArgumentNullException("pattern");

            return ExpectInteger(RedisCommandList.Sentinel, RedisCommandList.SentinelReset, pattern);
        }

        public RedisResult<RedisRoleInfo> Role()
        {
            var raw = ExpectArray(RedisCommandList.Role);
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

            var raw = ExpectArray(RedisCommandList.Sentinel, RedisCommandList.Sentinels, masterName.ToBytes());
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var items = RedisSentinelNodeInfo.ParseInfoResponse(rawValue);
                    if (!items.IsEmpty())
                        return new RedisResult<RedisSentinelNodeInfo[]>(items);
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

            return ExpectOK(RedisCommandList.Sentinel, RedisCommandList.SentinelSet, parameter, value);
        }

        public RedisResult<RedisSentinelSlaveInfo[]> Slaves(string masterName)
        {
            if (String.IsNullOrEmpty(masterName))
                throw new ArgumentNullException("masterName");

            var raw = ExpectArray(RedisCommandList.Sentinel, RedisCommandList.SentinelSlaves, masterName.ToBytes());
            if (!ReferenceEquals(raw, null))
            {
                var rawValue = raw.Value;
                if (!ReferenceEquals(rawValue, null) && rawValue.Type == RedisRawObjectType.Array)
                {
                    var items = rawValue.Items;
                    if (items != null)
                    {
                        var count = items.Count;
                        if (count > 0)
                        {
                            var list = new List<RedisSentinelSlaveInfo>(count);
                            for (var i = 0; i < count; i++)
                            {
                                var item = items[i];
                                if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Array)
                                {
                                    var info = new RedisSentinelSlaveInfo(item);
                                    list.Add(info);
                                }
                            }

                            if (list.Count > 0)
                                return new RedisResult<RedisSentinelSlaveInfo[]>(list.ToArray());
                        }
                    }
                }
            }
            return new RedisResult<RedisSentinelSlaveInfo[]>(null);
        }

        #endregion Methods
    }
}
