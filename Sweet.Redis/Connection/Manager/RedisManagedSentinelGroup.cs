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

        private Action<RedisSentinelMessage> m_OnSentinelMessage;

        private readonly List<RedisManagedSentinelListener> m_MonitoredSentinels = new List<RedisManagedSentinelListener>();

        #endregion Field Members

        #region .Ctors

        public RedisManagedSentinelGroup(RedisManagerSettings settings, string masterName, RedisManagedSentinelNode[] nodes,
                                         Action<object, RedisCardioPulseStatus> onPulseStateChange)
            : base(settings, RedisRole.Sentinel, nodes, onPulseStateChange)
        {
            masterName = (masterName ?? String.Empty).Trim();
            if (masterName.IsEmpty())
                throw new RedisFatalException(new ArgumentNullException("masterName"), RedisErrorCode.MissingParameter);

            m_MasterName = masterName;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnBeforeDispose(bool disposing, bool alreadyDisposed)
        {
            Interlocked.Exchange(ref m_OnSentinelMessage, null);
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
            return base.ExchangeNodes((RedisManagedSentinelNode[])nodes);
        }

        public void RegisterToMessageEvents(Action<RedisSentinelMessage> onSentinelMessage)
        {
            Interlocked.Exchange(ref m_OnSentinelMessage, onSentinelMessage);
        }

        public void Monitor(Action<object> onComplete)
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_MonitoringStatus, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                var attached = false;
                try
                {
                    var nodes = (RedisManagedSentinelNode[])Nodes;
                    if (!nodes.IsEmpty())
                    {
                        var monitoredSentinels = m_MonitoredSentinels;

                        attached = TryToMonitorOneOf(nodes, monitoredSentinels, onComplete, false);
                        if (!attached)
                            attached = TryToMonitorOneOf(nodes, monitoredSentinels, onComplete, true);
                    }
                }
                catch (Exception)
                {
                    Interlocked.Exchange(ref m_MonitoringStatus, RedisConstants.Zero);
                }
                finally
                {
                    if (!attached)
                        Interlocked.Exchange(ref m_MonitoringStatus, RedisConstants.Zero);
                }
            }
        }

        private bool TryToMonitorOneOf(RedisManagedSentinelNode[] nodes,
                                       List<RedisManagedSentinelListener> monitoredSentinels,
                                       Action<object> onComplete,
                                       bool getDownNodes)
        {
            try
            {
                if (!nodes.IsEmpty())
                {
                    var filteredNodes = nodes
                        .Where(node =>
                        {
                            return node.IsAlive() &&
                                       node.IsClosed == getDownNodes &&
                                       node.IsSeedAlive &&
                                       node.IsSeedDown == getDownNodes;
                        })
                        .ToArray();

                    foreach (var node in filteredNodes)
                    {
                        if (!Disposed &&
                           TryToMonitor(node, monitoredSentinels, onComplete))
                            return true;
                    }
                }
            }
            catch (Exception)
            { }
            return false;
        }

        private bool TryToMonitor(RedisManagedSentinelNode node,
                                  List<RedisManagedSentinelListener> monitoredSentinels,
                                  Action<object> onComplete)
        {
            if (node.IsAlive())
            {
                try
                {
                    var channel = node.Listener;
                    if (channel.IsAlive())
                    {
                        if (!channel.Ping())
                            return false;

                        channel.Subscribe(PubSubMessageReceived,
                                RedisCommandList.SentinelChanelSDownEntered,
                                RedisCommandList.SentinelChanelSDownExited,
                                RedisCommandList.SentinelChanelODownEntered,
                                RedisCommandList.SentinelChanelODownExited,
                                RedisCommandList.SentinelChanelSwitchMaster,
                                RedisCommandList.SentinelChanelSentinel);

                        monitoredSentinels.Add(channel);

                        channel.SetOnComplete((obj) =>
                            {
                                Interlocked.Exchange(ref m_MonitoringStatus, RedisConstants.Zero);
                                monitoredSentinels.Remove(channel);

                                channel.Ping();

                                if (onComplete != null)
                                    onComplete(node);
                            });
                        return true;
                    }
                }
                catch (Exception)
                {
                    node.IsClosed = true;
                }
            }
            return false;
        }

        private void PubSubMessageReceived(RedisPubSubMessage message)
        {
            if (!message.IsEmpty)
            {
                try
                {
                    InvokeOnSentinelMessage(message);
                }
                catch (Exception)
                { }
            }
        }

        private void InvokeOnSentinelMessage(RedisPubSubMessage message)
        {
            var messageType = ToSentinelMessageType(message.Channel);
            if (messageType == RedisSentinelMessageType.Undefined)
                return;

            var data = message.Data;
            if (data != null)
            {
                var msgText = Encoding.UTF8.GetString(data);

                if (!msgText.IsEmpty())
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
                            if (messageType == RedisSentinelMessageType.MasterChanged)
                            {
                                if (partsLength > 2)
                                {
                                    var onSentinelMessage = m_OnSentinelMessage;
                                    if (onSentinelMessage != null)
                                    {
                                        var masterName = parts[0];
                                        if (masterName == MasterName)
                                        {
                                            var oldEndPoint = ToEndPoint(parts[1], parts[2]);
                                            var newEndPoint = (partsLength > 4) ? ToEndPoint(parts[3], parts[4]) : RedisEndPoint.Empty;

                                            onSentinelMessage(new RedisSentinelMessage(messageType, masterName,
                                                                                       new RedisNodeInfo(newEndPoint, RedisRole.Master),
                                                                                       new RedisNodeInfo(oldEndPoint, RedisRole.Master)));
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
                                var onSentinelMessage = m_OnSentinelMessage;
                                if (onSentinelMessage != null)
                                {
                                    var instanceType = (parts[0] ?? String.Empty).ToLowerInvariant();

                                    var role = instanceType.ToRedisRole();
                                    switch (role)
                                    {
                                        case RedisRole.Master:
                                            {
                                                var masterName = parts[1];
                                                if (masterName == MasterName)
                                                {
                                                    var masterEndPoint = ToEndPoint(parts[2], parts[3]);

                                                    onSentinelMessage(new RedisSentinelMessage(messageType, masterName,
                                                                                               new RedisNodeInfo(masterEndPoint, RedisRole.Master),
                                                                                               new RedisNodeInfo(RedisEndPoint.Empty, RedisRole.Undefined)));
                                                }
                                                break;
                                            }
                                        case RedisRole.Slave:
                                        case RedisRole.Sentinel:
                                            {
                                                if (partsLength > 4)
                                                {
                                                    var masterName = parts[5];
                                                    if (masterName == MasterName)
                                                    {
                                                        var instanceName = parts[1];

                                                        var instanceEndPoint = ToEndPoint(parts[2], parts[3]);
                                                        var masterEndPoint = (partsLength > 7) ? ToEndPoint(parts[6], parts[7]) : RedisEndPoint.Empty;

                                                        if (instanceEndPoint.IsEmpty() && !instanceName.IsEmpty())
                                                        {
                                                            var nameParts = instanceName.Split(new[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                                                            if (!nameParts.IsEmpty())
                                                                instanceEndPoint = ToEndPoint(nameParts[0], nameParts[1]);
                                                        }

                                                        onSentinelMessage(new RedisSentinelMessage(messageType, masterName,
                                                                                                   new RedisNodeInfo(instanceEndPoint, role, instanceName),
                                                                                                   new RedisNodeInfo(masterEndPoint, !masterEndPoint.IsEmpty() ? RedisRole.Master : RedisRole.Undefined)));
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

        private RedisSentinelMessageType ToSentinelMessageType(string channel)
        {
            if (!channel.IsEmpty())
            {
                switch (channel)
                {
                    case RedisConstants.ODownEntered:
                        return RedisSentinelMessageType.ObjectivelyDown;
                    case RedisConstants.ODownExited:
                        return RedisSentinelMessageType.ObjectivelyUp;
                    case RedisConstants.SDownEntered:
                        return RedisSentinelMessageType.SubjectivelyDown;
                    case RedisConstants.SDownExited:
                        return RedisSentinelMessageType.SubjectivelyUp;
                    case RedisConstants.SwitchMaster:
                        return RedisSentinelMessageType.MasterChanged;
                    case RedisConstants.SentinelDiscovered:
                        return RedisSentinelMessageType.SentinelDiscovered;
                    default:
                        return RedisSentinelMessageType.Undefined;
                }
            }
            return RedisSentinelMessageType.Undefined;
        }

        private static RedisEndPoint ToEndPoint(string ip, string port)
        {
            if (!ip.IsEmpty() && !port.IsEmpty())
            {
                int p;
                if (int.TryParse(port, out p))
                {
                    IPAddress ipAddr;
                    if (IPAddress.TryParse(ip, out ipAddr))
                        return new RedisEndPoint(ip, p);

                }
            }
            return RedisEndPoint.Empty;
        }

        public void Quit()
        {
            if (Interlocked.CompareExchange(ref m_MonitoringStatus, RedisConstants.Zero, RedisConstants.One) ==
                RedisConstants.One)
            {
                var monitoredSentinels = m_MonitoredSentinels;
                if (monitoredSentinels != null && monitoredSentinels.Count > 0)
                {
                    var sentinels = monitoredSentinels.ToArray();
                    if (!sentinels.IsEmpty())
                    {
                        foreach (var sentinel in sentinels)
                        {
                            try
                            {
                                if (sentinel.IsAlive())
                                    sentinel.Unsubscribe();
                            }
                            catch (Exception)
                            { }
                            finally
                            {
                                if (sentinel != null)
                                    monitoredSentinels.Remove(sentinel);
                            }
                        }
                    }
                }
            }
        }

        #endregion Methods
    }
}
