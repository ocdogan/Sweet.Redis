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
    /*
    Master down:
    ------------
    +sdown master mymaster 127.0.0.1 6379
    +switch-master mymaster 127.0.0.1 6379 127.0.0.1 6380
    +sdown slave 127.0.0.1:6380 127.0.0.1 6380 @ mymaster 127.0.0.1 6379

    Slave up:
    ------------
    -sdown slave 127.0.0.1:6380 127.0.0.1 6380 @ mymaster 127.0.0.1 6379

    Slave down:
    ------------
    +sdown slave 127.0.0.1:6380 127.0.0.1 6380 @ mymaster 127.0.0.1 6379

    -odown master mymaster 127.0.0.1 6379
    +odown master mymaster 127.0.0.1 6379 #quorum 2/2
    
    +switch-master mymaster 127.0.0.1 6380 127.0.0.1 6379
    
    New Sentinel:
    -------------
    +sdown sentinel 127.0.0.1:26381 127.0.0.1 26381 @ mymaster 127.0.0.1 6379
    -dup-sentinel master mymaster 127.0.0.1 6381 #duplicate of 127.0.0.1:26381 or cab1c287ec59309126ad1a63b354ba132bb4e55b
    +sentinel sentinel 127.0.0.1:26381 127.0.0.1 26379 @ mymaster 127.0.0.1 6379
     */
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

        public void RegisterMessageEvents(Action<RedisMasterSwitchedMessage> onSwitchMaster,
                                          Action<RedisNodeStateChangedMessage> onInstanceStateChange)
        {
            Interlocked.Exchange(ref m_OnSwitchMaster, onSwitchMaster);
            Interlocked.Exchange(ref m_OnInstanceStateChange, onInstanceStateChange);
        }

        public void Monitor(Action<object> onComplete)
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_MonitoringStatus, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                var monitoring = false;
                try
                {
                    var nodes = Nodes;
                    if (!nodes.IsEmpty())
                    {
                        var monitoredPools = m_MonitoredPools;

                        monitoring = TryToMonitorOneOf(nodes, monitoredPools, onComplete, false);
                        if (!monitoring)
                            monitoring = TryToMonitorOneOf(nodes, monitoredPools, onComplete, true);
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

        private bool TryToMonitorOneOf(RedisManagedNode[] nodes,
                                List<RedisManagedConnectionPool> monitoredPools,
                                Action<object> onComplete,
                                 bool downNodes)
        {
            if (!nodes.IsEmpty())
            {
                var filteredNodes = nodes
                    .Where(node =>
                    {
                        if (node.IsAlive())
                        {
                            var pool = node.Pool;
                            return pool.IsAlive() && pool.IsDown == downNodes;
                        }
                        return false;
                    })
                    .ToArray();

                foreach (var node in filteredNodes)
                {
                    if (!Disposed &&
                       TryToMonitor(node, monitoredPools, onComplete))
                        return true;
                }
            }
            return false;
        }

        private bool TryToMonitor(RedisManagedNode node,
                                List<RedisManagedConnectionPool> monitoredPools,
                                Action<object> onComplete)
        {
            if (node.IsAlive())
            {
                var pool = node.Pool;
                if (pool.IsAlive())
                {
                    var channel = pool.PubSubChannel;
                    if (channel.IsAlive())
                    {
                        if (!Ping(pool))
                            return false;

                        try
                        {
                            channel.Subscribe(PubSubMessageReceived,
                                    RedisCommandList.SentinelChanelSDownEntered,
                                    RedisCommandList.SentinelChanelSDownExited,
                                    RedisCommandList.SentinelChanelODownEntered,
                                    RedisCommandList.SentinelChanelODownExited,
                                    RedisCommandList.SentinelChanelSwitchMaster,
                                    RedisCommandList.SentinelChanelSentinel);

                            monitoredPools.Add(pool);

                            var pubSubChannel = pool.PubSubChannel as RedisPubSubChannel;
                            if (pubSubChannel.IsAlive())
                            {
                                pubSubChannel.SetOnComplete((obj) =>
                                {
                                    Interlocked.Exchange(ref m_MonitoringStatus, RedisConstants.Zero);
                                    monitoredPools.Remove(pool);

                                    Ping(pool);

                                    if (onComplete != null)
                                        onComplete(node);
                                });
                            }
                            return true;
                        }
                        catch (Exception)
                        {
                            pool.SDown = true;
                            pool.ODown = true;
                        }
                    }
                }
            }
            return false;
        }

        private bool Ping(RedisManagedConnectionPool pool)
        {
            if (pool.IsAlive())
            {
                try
                {
                    using (var db = pool.GetDb(-1))
                        db.Connection.Ping();
                    return true;
                }
                catch (Exception)
                {
                    pool.SDown = true;
                    pool.ODown = true;
                }
            }
            return false;
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
                        channel == RedisConstants.SwitchMaster ||
                        channel == RedisConstants.SentinelDiscovered)
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
                            /*
                            +switch-master mymaster 127.0.0.1 6379 127.0.0.1 6380
                             */
                            if (channel == RedisConstants.SwitchMaster)
                            {
                                if (partsLength > 2)
                                {
                                    var onSwitchMaster = m_OnSwitchMaster;
                                    if (onSwitchMaster != null)
                                    {
                                        var masterName = parts[0];
                                        if (masterName == MasterName)
                                        {
                                            var oldEndPoint = ToEndPoint(parts[1], parts[2]);
                                            var newEndPoint = (partsLength > 4) ? ToEndPoint(parts[3], parts[4]) : null;

                                            onSwitchMaster(new RedisMasterSwitchedMessage(masterName, oldEndPoint, newEndPoint));
                                        }
                                    }
                                }
                                return;
                            }

                            /* Samples:
                            +sdown master mymaster 127.0.0.1 6380
                            +sdown slave 127.0.0.1:6380 127.0.0.1 6380 @ mymaster 127.0.0.1 6379
                            -sdown slave 127.0.0.1:6380 127.0.0.1 6380 @ mymaster 127.0.0.1 6379

                            +odown master mymaster 127.0.0.1 6379 #quorum 2/2
                            -odown master mymaster 127.0.0.1 6379

                            +sdown sentinel 127.0.0.1:26381 127.0.0.1 26381 @ mymaster 127.0.0.1 6379
                            -dup-sentinel master mymaster 127.0.0.1 6381 #duplicate of 127.0.0.1:26381 or cab1c287ec59309126ad1a63b354ba132bb4e55b
                            +sentinel sentinel 127.0.0.1:26381 127.0.0.1 26379 @ mymaster 127.0.0.1 6379
                             */
                            if (partsLength > 3)
                            {
                                var onInstanceStateChange = m_OnInstanceStateChange;
                                if (onInstanceStateChange != null)
                                {
                                    var instanceType = (parts[0] ?? String.Empty).ToLowerInvariant();

                                    switch (instanceType)
                                    {
                                        case "master":
                                            {
                                                var masterName = parts[1];
                                                if (masterName == MasterName)
                                                {
                                                    var masterEndPoint = ToEndPoint(parts[2], parts[3]);
                                                    onInstanceStateChange(new RedisNodeStateChangedMessage(channel, instanceType, 
                                                            masterName, masterEndPoint, null, null));
                                                }
                                                break;
                                            }
                                        case "slave":
                                        case "sentinel":
                                            {
                                                if (partsLength > 4)
                                                {
                                                    var masterName = parts[5];
                                                    if (masterName == MasterName)
                                                    {
                                                        var instanceName = parts[1];
                                                        var instanceEndPoint = ToEndPoint(parts[2], parts[3]);
                                                        var masterEndPoint = (partsLength > 7) ? ToEndPoint(parts[6], parts[7]) : null;

                                                        if (instanceEndPoint.IsEmpty() && !String.IsNullOrEmpty(instanceName))
                                                        {
                                                            var nameParts = instanceName.Split(new[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                                                            if (!nameParts.IsEmpty())
                                                                instanceEndPoint = ToEndPoint(nameParts[0], nameParts[1]);
                                                        }

                                                        onInstanceStateChange(new RedisNodeStateChangedMessage(channel, instanceType, 
                                                                instanceName, instanceEndPoint, masterName, masterEndPoint));
                                                    }
                                                }
                                                break;
                                            }
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
                        if (!pools.IsEmpty())
                        {
                            foreach (var pool in pools)
                            {
                                try
                                {
                                    if (pool.IsAlive())
                                    {
                                        var channel = pool.PubSubChannel;
                                        if (channel.IsAlive())
                                            channel.Unsubscribe();
                                    }
                                }
                                catch (Exception)
                                { }
                                finally
                                {
                                    if (pool != null)
                                        monitoredPools.Remove(pool);
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
