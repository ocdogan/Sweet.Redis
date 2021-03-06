﻿#region License
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
using System.Threading;

namespace Sweet.Redis
{
    public class RedisConnectionProvider : RedisCommandExecuter, IRedisConnectionProvider,
            IRedisConnectionInfoProvider, IRedisCommandExecuter, IRedisNamedObject, IRedisIdentifiedObject
    {
        #region Constants

        protected const int ConnectionSpinStepTimeoutMillisecs = 2;

        #endregion Constants

        #region Field Members

        private string m_Name;
        private RedisConnectionLimiter m_ConnectionLimiter;

        #endregion Field Members

        #region .Ctors

        protected RedisConnectionProvider(string name, RedisConnectionSettings settings,
            Func<int, RedisConnectionLimiter> connectionLimiter = null)
            : base(settings)
        {
            name = (name ?? String.Empty).Trim();
            m_Name = !name.IsEmpty() ? name :
                String.Format("{0}, {1}", GetType().Name, Id.ToString());

            if (connectionLimiter == null)
                connectionLimiter = NewConnectionLimiter;

            var maxConnectionCount = Math.Max(RedisConstants.MinConnectionCount, GetMaxConnectionCount());
            m_ConnectionLimiter = connectionLimiter(maxConnectionCount) ??
                                                   new RedisConnectionLimiter(maxConnectionCount);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            var connectionLimiter = Interlocked.Exchange(ref m_ConnectionLimiter, null);
            if (connectionLimiter != null)
                connectionLimiter.Dispose();
        }

        #endregion Destructors

        #region Properties

        public virtual int AvailableCount
        {
            get
            {
                if (Disposed)
                    return 0;
                var connectionLimiter = m_ConnectionLimiter;
                return (connectionLimiter != null) ? connectionLimiter.AvailableCount : 0;
            }
        }

        public virtual int InUseCount
        {
            get
            {
                if (Disposed)
                    return 0;
                var connectionLimiter = m_ConnectionLimiter;
                return (connectionLimiter != null) ? connectionLimiter.InUseCount : 0;
            }
        }

        public virtual bool IsDown
        {
            get { return !Disposed; }
            protected internal set { }
        }

        public virtual int SpareCount { get { return 0; } }

        public string Name
        {
            get { return m_Name; }
        }

        #endregion Properties

        #region Methods

        #region Initializer

        protected override void InitExecuter()
        {
        }

        #endregion Initializer

        #region Pulse

        protected internal virtual bool Ping(bool forceNewConnection = false)
        {
            if (!Disposed)
            {
                try
                {
                    var result = false;
                    using (var connection = NewConnection(Settings))
                    {
                        using (var cmd = new RedisCommand(-1, RedisCommandList.Ping))
                        {
                            var pong = cmd.ExpectSimpleString(connection);
                            result = (pong == RedisConstants.PONG);
                        }
                    }
                    return result;
                }
                catch (Exception)
                { }
            }
            return false;
        }

        #endregion Pulse

        #region Connection

        protected virtual void ApplyRole(RedisRole role)
        {
        }

        protected virtual void OnBeforeConnect(int dbIndex, RedisRole expectedRole)
        {
        }

        protected virtual int GetMaxConnectionCount()
        {
            return RedisConstants.MinConnectionCount;
        }

        protected virtual RedisConnectionLimiter NewConnectionLimiter(int maxCount)
        {
            maxCount = Math.Max(1, Math.Min(maxCount, RedisConstants.MaxConnectionCount));
            return new RedisConnectionLimiter(maxCount);
        }

        protected virtual int GetConnectionSpinStepTimeout()
        {
            return ConnectionSpinStepTimeoutMillisecs;
        }

        protected virtual void OnConnectionRetry(RedisConnectionRetryEventArgs e)
        { }

        protected virtual void OnConnectionLimitExceed(RedisConnectionRetryEventArgs e)
        { }

        protected virtual void OnConnectionTimeout(RedisConnectionRetryEventArgs e)
        { }

        IRedisConnection IRedisConnectionProvider.Connect(int dbIndex, RedisRole expectedRole)
        {
            return this.Connect(dbIndex, expectedRole);
        }

        protected internal override IRedisConnection Connect(int dbIndex, RedisRole expectedRole)
        {
            ValidateNotDisposed();

            OnBeforeConnect(dbIndex, expectedRole);

            var settings = (Settings ?? RedisPoolSettings.Default);

            var spinStepTimeoutMs = GetConnectionSpinStepTimeout();

            var connectionTimeout = settings.ConnectionTimeout;
            connectionTimeout = connectionTimeout <= 0 ? RedisConstants.MaxConnectionTimeout : connectionTimeout;

            var retryCountLimit = (int)Math.Ceiling((double)settings.ConnectionWaitTimeout / spinStepTimeoutMs);
            var retryInfo = new RedisConnectionRetryEventArgs(retryCountLimit, spinStepTimeoutMs,
                                                              connectionTimeout, connectionTimeout);

            var maxConnectionCount = Math.Max(RedisConstants.MinConnectionCount, GetMaxConnectionCount());

            var limiterWait = (maxConnectionCount < 2) ? 0 : retryInfo.SpinStepTimeoutMs;

            while (retryInfo.RemainingTime > 0)
            {
                var signaled = m_ConnectionLimiter.Wait(limiterWait);
                if (signaled)
                {
                    try
                    {
                        return NewConnection(DequeueSocket(dbIndex, expectedRole), dbIndex, expectedRole, true);
                    }
                    catch (Exception e)
                    {
                        m_ConnectionLimiter.Release();
                        if (e.IsSocketError())
                            throw;
                    }
                }

                retryInfo.Entered();
                OnConnectionRetry(retryInfo);

                if (!retryInfo.ContinueToSpin ||
                    retryInfo.CurrentRetryCount >= retryInfo.RetryCountLimit)
                {
                    OnConnectionLimitExceed(retryInfo);
                    if (retryInfo.ThrowError)
                        throw new RedisFatalException("Wait retry count exited the given maximum limit", RedisErrorCode.ConnectionError);
                    return null;
                }
            }

            OnConnectionTimeout(retryInfo);
            if (retryInfo.ThrowError)
                throw new RedisFatalException("Connection timeout occured while trying to connect", RedisErrorCode.ConnectionError);
            return null;
        }

        protected virtual IRedisConnection NewConnection(RedisConnectionSettings settings)
        {
            return null;
        }

        protected virtual IRedisConnection NewConnection(RedisSocket socket, int dbIndex, RedisRole expectedRole, bool connectImmediately = true)
        {
            return null;
        }

        protected virtual RedisSocket DequeueSocket(int dbIndex, RedisRole expectedRole)
        {
            return null;
        }

        internal void ReuseSocket(RedisSocket socket)
        {
            OnReleaseSocket(null, socket);
        }

        #endregion Connection

        #region Socket Release

        protected void Release()
        {
            var connectionLimiter = m_ConnectionLimiter;
            if (connectionLimiter != null)
                connectionLimiter.Release();
        }

        protected virtual void OnReleaseSocket(IRedisConnection connection, RedisSocket socket)
        {
            ValidateNotDisposed();
            try
            {
                CompleteSocketRelease(connection, socket);
            }
            finally
            {
                Release();
            }
        }

        protected virtual void CompleteSocketRelease(IRedisConnection connection, RedisSocket socket)
        { }

        #endregion Socket Release

        #endregion Methods
    }
}
