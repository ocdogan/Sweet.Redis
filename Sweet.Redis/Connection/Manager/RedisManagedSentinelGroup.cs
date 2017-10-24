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
using System.Linq;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisManagedSentinelGroup : RedisManagedNodesGroup
    {
        #region Constants

        private const string SDownEntered = "+sdown";
        private const string SDownExited = "-sdown";
        private const string ODownEntered = "+odown";
        private const string ODownExited = "-odown";
        private const string SwitchMaster = "+switch-master";

        #endregion Constants

        #region Field Members

        private long m_MonitoringStatus;
        private List<RedisManagedConnectionPool> m_MonitoredPools = new List<RedisManagedConnectionPool>();
        private Dictionary<RedisEndPoint, RedisManagedConnectionPool> m_MonitoredPoolIndex = 
            new Dictionary<RedisEndPoint, RedisManagedConnectionPool>();

        #endregion Field Members

        #region .Ctors

        public RedisManagedSentinelGroup(RedisManagedNode[] nodes)
            : base(RedisRole.Sentinel, nodes)
        { }

        #endregion .Ctors

        #region Destructors

        protected override void OnBeforeDispose(bool disposing, bool alreadyDisposed)
        {
 	        base.OnBeforeDispose(disposing, alreadyDisposed);
            Quit();
        }

        #endregion Destructors

        #region Properties

        public bool Monitoring
        {
            get
            {
                return Interlocked.Read(ref m_MonitoringStatus) == RedisConstants.One;
            }
        }

        #endregion Properties

        #region Methods

        public override RedisManagedNode[] ExchangeNodes(RedisManagedNode[] nodes)
        {
            var oldNodes = base.ExchangeNodes(nodes);
            return oldNodes;
        }

        public void Monitor()
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_MonitoringStatus, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                var monitoring = false;
                try
                {
                    var nodes = Nodes;
                    if (nodes != null && nodes.Length > 0)
                    {
                        var monitoredPools = m_MonitoredPools;
                        var monitoredPoolIndex = m_MonitoredPoolIndex;

                        foreach (var node in nodes)
                        {
                            try
                            {
                                if (node != null && !node.Disposed)
                                {
                                    var pool = node.Pool;
                                    if (pool != null && !pool.Disposed)
                                    {
                                        var channel = pool.PubSubChannel;
                                        if (channel != null)
                                        {
                                            channel.Subscribe(PubSubMessageReceived, 
                                                RedisCommandList.SentinelChanelSDownEntered, 
                                                RedisCommandList.SentinelChanelSDownExited, 
                                                RedisCommandList.SentinelChanelODownEntered, 
                                                RedisCommandList.SentinelChanelODownExited, 
                                                RedisCommandList.SentinelChanelSwitchMaster);

                                            monitoring = true;
                                            if (monitoredPools != null)
                                                monitoredPools.Add(pool);

                                            if (monitoredPoolIndex != null)
                                            {
                                                var ep = pool.EndPoint;
                                                if (ep != null && !ep.IsEmpty)
                                                    monitoredPoolIndex[ep] = pool;
                                            }
                                        }
                                    }
                                }
                            }
                            catch (Exception)
                            { }
                        }
                    }
                }
                catch (Exception)
                {
                    Interlocked.Exchange(ref m_MonitoringStatus, RedisConstants.Zero);
                }
                finally
                {
                    if (!monitoring)
                        Interlocked.Exchange(ref m_MonitoringStatus, RedisConstants.Zero);
                }
            }
        }

        private void PubSubMessageReceived(RedisPubSubMessage message)
        {
            if (!message.IsEmpty)
            {
                var instance = GetInstanceFromMessage(message);
            }
        }

        private RedisEndPoint GetInstanceFromMessage(RedisPubSubMessage message)
        {
            var channel = message.Channel;
	        if (channel == SDownEntered || channel == SDownExited ||
                channel == SDownEntered || channel == ODownExited ||
                channel == SwitchMaster)
            {
                var data = message.Data;
                if (data != null)
                {
                    var str = Encoding.UTF8.GetString(data);
                    if (!String.IsNullOrEmpty(str))
                    {
                        var parts = str.Split(new [] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length > 0)
                        {
                            var instance = (string)null;
                            if (channel == SwitchMaster)
                                instance = parts[0];
                            else if (parts.Length > 1 && parts[0] == "master")
                                instance = parts[1];
                        }
                    }
                }
	        }
            return null;
        }

        public void Quit()
        {
            if (Interlocked.CompareExchange(ref m_MonitoringStatus, RedisConstants.Zero, RedisConstants.One) ==
                RedisConstants.One)
            {
                var monitoredPools = m_MonitoredPools;
                if (monitoredPools != null && monitoredPools.Count > 0)
                {
                    var monitoredPoolIndex = m_MonitoredPoolIndex;

                    var pools = monitoredPools.ToArray();
                    if (pools != null && pools.Length > 0)
                    {
                        foreach (var pool in pools)
                        {
                            if (pool != null)
                            {
                                try
                                {
                                    if (!pool.Disposed)
                                    {
                                        var channel = pool.PubSubChannel;
                                        if (channel != null)
                                            channel.Unsubscribe();
                                    }
                                }
                                catch (Exception)
                                { }
                                finally
                                {
                                    var ep = pool.EndPoint;
                                    if (ep != null && 
                                        monitoredPoolIndex.ContainsKey(ep))
                                        monitoredPoolIndex.Remove(ep);
                                }
                            }
                        }
                    }
                }
            }
        }

        #endregion Methods
    }
}
