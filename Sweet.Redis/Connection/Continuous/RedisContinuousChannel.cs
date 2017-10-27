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
using System.Threading;

namespace Sweet.Redis
{
    public class RedisContinuousChannel<T> : RedisInternalDisposable
    {
        #region RedisChannelSubscriptions

        private class RedisChannelSubscriptions : RedisCallbackHub<T>
        {
            #region Methods

            public override void Invoke(T message)
            {
                if (!ReferenceEquals(message, null))
                {
                    var callbacks = CallbacksOf("*");
                    if (callbacks != null && callbacks.Count > 0)
                    {
                        foreach (var callback in callbacks)
                        {
                            try
                            {
                                callback.InvokeAsync(message);
                            }
                            catch (Exception)
                            { }
                        }
                    }
                }
            }

            #endregion Methods
        }

        #endregion RedisChannelSubscriptions

        #region Field Members

        private event Action<object> m_OnComplete;

        private RedisPoolSettings m_Settings;
        private IRedisConnectionProvider m_ConnectionProvider;

        private readonly object m_SubscriptionLock = new object();
        private RedisChannelSubscriptions m_Subscriptions = new RedisChannelSubscriptions();

        #endregion Field Members

        #region .Ctors

        internal RedisContinuousChannel(RedisPoolSettings settings)
        {
            if (settings == null)
                throw new RedisFatalException(new ArgumentNullException("settings"), RedisErrorCode.MissingParameter);

            m_Settings = settings;

            Func<IRedisConnectionProvider> factory = () => { return NewConnectionProvider(); };
            m_ConnectionProvider = factory();
        }

        #endregion .Ctors

        #region Destructors

        protected void RegisterOnComplete(Action<object> callback)
        {
            if (callback != null)
                m_OnComplete += callback;
        }

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_OnComplete, null);

            var connectionProvider = Interlocked.Exchange(ref m_ConnectionProvider, null);
            if (connectionProvider != null)
                connectionProvider.Dispose();
        }

        #endregion Destructors

        #region Properties

        protected IRedisConnectionProvider ConnectionProvider
        {
            get { return m_ConnectionProvider; }
        }

        public RedisEndPoint EndPoint
        {
            get
            {
                var connectionProvider = m_ConnectionProvider;
                if (connectionProvider != null)
                    return connectionProvider.EndPoint;

                var settings = Settings;
                if (settings != null)
                {
                    var endPoints = settings.EndPoints;
                    if (endPoints != null && endPoints.Length > 0)
                    {
                        foreach (var ep in endPoints)
                            if (ep != null)
                                return (RedisEndPoint)ep.Clone();
                    }
                }

                return RedisEndPoint.Empty;
            }
        }

        public RedisConnectionSettings Settings { get { return m_Settings; } }

        #endregion Properties

        #region Methods

        protected virtual IRedisConnectionProvider NewConnectionProvider()
        {
            return new RedisContinuousConnectionProvider(GetProviderName(), m_Settings, ResponseReceived);
        }

        protected virtual string GetProviderName()
        {
            return String.Format("{0}, {1}", GetType().Name, RedisIDGenerator<RedisContinuousConnectionProvider>.NextId());
        }

        protected virtual IRedisConnection Connect(RedisRole role, bool beginReveive = false)
        {
            ValidateNotDisposed();

            var connectionProvider = m_ConnectionProvider;
            if (connectionProvider != null)
            {
                var connection = connectionProvider.Connect(-1, role);

                if (connection != null && !connection.Connected)
                    connection.Connect();

                OnConnect(connection, beginReveive);

                return connection;
            }
            return null;
        }

        protected virtual void OnConnect(IRedisConnection connection, bool beginReveive = false)
        {
            if (beginReveive)
            {
                var continuousConnection = connection as IRedisContinuousConnection;
                if (continuousConnection != null)
                {
                    continuousConnection.BeginReceive((conn) =>
                    {
                        var onComplete = m_OnComplete;
                        if (onComplete != null)
                            onComplete(this);
                    });
                }
            }
        }

        protected virtual bool TryConvertResponse(IRedisRawResponse response, out T value)
        {
            value = default(T);
            return true;
        }

        protected virtual void ResponseReceived(IRedisRawResponse response)
        {
            if (CanSendResponse(response))
            {
                var subscriptions = m_Subscriptions;
                if (subscriptions != null)
                {
                    T message;
                    if (TryConvertResponse(response, out message))
                        subscriptions.Invoke(message);
                }
            }
        }

        protected virtual bool CanSendResponse(IRedisRawResponse response)
        {
            return !ReferenceEquals(response, null);
        }

        protected virtual void SendAsync(byte[] cmd, params byte[][] parameters)
        {
            if (cmd != null && cmd.Length > 0)
            {
                Action action = () =>
                {
                    var connection = Connect(cmd.CommandRole(), CanBeginReceive(cmd));
                    if (connection != null)
                    {
                        try
                        {
                            if (connection.Connected)
                            {
                                var command = new RedisCommand(0, cmd, RedisCommandType.SendNotReceive, parameters);
                                connection.SendAsync(command)
                                    .ContinueWith(t =>
                                    {
                                        var quitMessage = (command.Command == RedisCommandList.Quit);
                                        command.Dispose();

                                        if (quitMessage)
                                        {
                                            var provider = m_ConnectionProvider;
                                            if (provider.IsAlive())
                                            {
                                                var scProvider = provider as RedisSingleConnectionProvider;
                                                if (scProvider != null)
                                                    scProvider.DisposeConnection();
                                            }
                                        }
                                    });
                            }
                        }
                        finally
                        {
                            connection.ReleaseSocket();
                        }
                    }
                };
                action.InvokeAsync();
            }
        }

        protected virtual bool CanBeginReceive(byte[] cmd)
        {
            return false;
        }

        public void Subscribe(Action<T> callback)
        {
            if (callback == null)
                throw new ArgumentNullException("callback");

            ValidateNotDisposed();

            var subscriptions = m_Subscriptions;
            if (subscriptions != null)
            {
                lock (m_SubscriptionLock)
                {
                    if (!subscriptions.Exists("*"))
                    {
                        subscriptions.Register("*", callback);
                    }
                }
                OnSubscribe();
            }
        }

        protected virtual void OnSubscribe()
        { }

        public void UnregisterSubscription(Action<T> callback)
        {
            if (callback != null)
            {
                lock (m_SubscriptionLock)
                {
                    m_Subscriptions.Unregister(callback);
                }
                OnUnsubscribe(callback);
            }
        }

        protected virtual void OnUnsubscribe(Action<T> callback)
        { }

        public void Quit()
        {
            if (!Disposed)
            {
                var provider = m_ConnectionProvider;
                if (provider != null && provider.IsAlive())
                {
                    var infoProvider = provider as IRedisConnectionInfoProvider;
                    if (infoProvider == null || infoProvider.SpareCount > 0)
                        SendAsync(RedisCommandList.Quit);
                }
            }
        }

        #endregion Methods
    }
}
