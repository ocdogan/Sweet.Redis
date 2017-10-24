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
using System.Net;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisManagedSentinelGroup : RedisManagedNodesGroup
    {
        #region Field Members

        private string m_MasterName;
        private long m_MonitoringStatus;

        private Action<RedisMasterSwitchedMessage> m_OnSwitchMaster;
        private Action<RedisNodeStateChangedMessage> m_OnInstanceStateChange;

        private List<RedisManagedConnectionPool> m_MonitoredPools = new List<RedisManagedConnectionPool>();

        #endregion Field Members

        #region .Ctors

        public RedisManagedSentinelGroup(string masterName, RedisManagedNode[] nodes)
            : base(RedisRole.Sentinel, nodes)
        {
            masterName = (masterName ?? String.Empty).Trim();
            if (String.IsNullOrEmpty(masterName))
                throw new RedisFatalException(new ArgumentNullException("masterName"), RedisErrorCode.MissingParameter);

            m_MasterName = masterName;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnBeforeDispose(bool disposing, bool alreadyDisposed)
        {
            Interlocked.Exchange(ref m_OnSwitchMaster, null);
            Interlocked.Exchange(ref m_OnInstanceStateChange, null);

            base.OnBeforeDispose(disposing, alreadyDisposed);
            Quit();
        }

        #endregion Destructors

        #region Properties

        public string MasterName { get { return m_MasterName; } }

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

        public void Monitor(Action<RedisMasterSwitchedMessage> onSwitchMaster, Action<RedisNodeStateChangedMessage> onInstanceStateChange)
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
                                            using (var db = pool.GetDb(-1))
                                                db.Connection.Ping();

                                            channel.Subscribe(PubSubMessageReceived,
                                                RedisCommandList.SentinelChanelSDownEntered,
                                                RedisCommandList.SentinelChanelSDownExited,
                                                RedisCommandList.SentinelChanelODownEntered,
                                                RedisCommandList.SentinelChanelODownExited,
                                                RedisCommandList.SentinelChanelSwitchMaster);

                                            monitoring = true;
                                            monitoredPools.Add(pool);

                                            break;
                                        }
                                    }
                                }
                            }
                            catch (Exception e)
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
                    else
                    {
                        Interlocked.Exchange(ref m_OnSwitchMaster, onSwitchMaster);
                        Interlocked.Exchange(ref m_OnInstanceStateChange, onInstanceStateChange);
                    }
                }
            }
        }

        private void PubSubMessageReceived(RedisPubSubMessage message)
        {
            try
            {
                if (!message.IsEmpty)
                {
                    var channel = message.Channel;

                    if (channel == RedisConstants.SDownEntered || 
                        channel == RedisConstants.SDownExited ||
                        channel == RedisConstants.SDownEntered || 
                        channel == RedisConstants.ODownExited ||
                        channel == RedisConstants.SwitchMaster)
                        InvokeCallback(message);
                }
            }
            catch (Exception)
            { }
        }

        private void InvokeCallback(RedisPubSubMessage message)
        {
            var channel = message.Channel;

            var data = message.Data;
            if (data != null)
            {
                var msgText = Encoding.UTF8.GetString(data);

                if (!String.IsNullOrEmpty(msgText))
                {
                    var parts = msgText.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                    if (parts != null)
                    {
                        var partsLength = parts.Length;
                        if (partsLength > 0)
                        {
                            if (channel == RedisConstants.SwitchMaster) // "+switch-master"
                            {
                                if (partsLength > 2)
                                {
                                    var onSwitchMaster = m_OnSwitchMaster;
                                    if (onSwitchMaster != null)
                                    {
                                        var masterName = parts[0];
                                        if (masterName == MasterName)
                                        {
                                            var oldEP = ToEndPoint(parts[1], parts[2]);

                                            RedisEndPoint newEP = null;
                                            if (partsLength > 4)
                                                newEP = ToEndPoint(parts[3], parts[4]);

                                            onSwitchMaster(new RedisMasterSwitchedMessage(masterName, oldEP, newEP));
                                        }
                                    }
                                }
                                return;
                            }

                            // "+sdown", "-sdown", "+odown", "-odown"
                            if (partsLength > 7)
                            {
                                var onInstanceStateChange = m_OnInstanceStateChange;
                                if (onInstanceStateChange != null)
                                {
                                    var masterName = parts[5];
                                    if (masterName == MasterName)
                                    {
                                        var instanceType = parts[0];
                                        var name = parts[1];

                                        var endPoint = ToEndPoint(parts[2], parts[3]);
                                        if (endPoint == null && !String.IsNullOrEmpty(name))
                                        {
                                            parts = name.Split(new[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                                            if (parts != null && partsLength > 1)
                                                endPoint = ToEndPoint(parts[0], parts[1]);
                                        }

                                        var masterEP = ToEndPoint(parts[6], parts[7]);

                                        onInstanceStateChange(new RedisNodeStateChangedMessage(channel, instanceType, name, masterName, endPoint, masterEP));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        private static RedisEndPoint ToEndPoint(string ip, string port)
        {
            if (!String.IsNullOrEmpty(ip) && !String.IsNullOrEmpty(port))
            {
                int p;
                if (int.TryParse(port, out p))
                {
                    IPAddress ipAddr;
                    if (IPAddress.TryParse(ip, out ipAddr))
                        return new RedisEndPoint(ip, p);

                }
            }
            return null;
        }

        public void Quit()
        {
            if (Interlocked.CompareExchange(ref m_MonitoringStatus, RedisConstants.Zero, RedisConstants.One) ==
                RedisConstants.One)
            {
                try
                {
                    var monitoredPools = m_MonitoredPools;
                    if (monitoredPools.Count > 0)
                    {
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
                                        monitoredPools.Remove(pool);
                                    }
                                }
                            }
                        }
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref m_OnSwitchMaster, null);
                    Interlocked.Exchange(ref m_OnInstanceStateChange, null);
                }
            }
        }

        #endregion Methods
    }
}
