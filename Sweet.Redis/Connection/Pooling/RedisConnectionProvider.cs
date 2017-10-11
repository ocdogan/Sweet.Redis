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
using System.Threading;

namespace Sweet.Redis
{
    public class RedisConnectionProvider : RedisDisposable, IRedisConnectionProvider, IRedisCommandExecuter
    {
        #region Constants

        protected const int ConnectionSpinStepTimeoutMillisecs = 2;

        #endregion Constants

        #region Field Members

        private string m_Name;
        private RedisSettings m_Settings;
        private RedisConnectionLimiter m_ConnectionLimiter;

        #endregion Field Members

        #region .Ctors

        protected RedisConnectionProvider(string name, RedisSettings settings = null,
            Func<int, RedisConnectionLimiter> connectionLimiter = null)
        {
            m_Settings = settings ?? RedisSettings.Default;

            name = (name ?? String.Empty).Trim();
            m_Name = !String.IsNullOrEmpty(name) ? name : 
                String.Format("{0}, {1}", GetType().Name, Guid.NewGuid().ToString("N").ToUpper());

            if (connectionLimiter == null)
                connectionLimiter = (maxCount) => NewConnectionLimiter(maxCount);

            m_ConnectionLimiter = connectionLimiter(settings.MaxCount) ??
                                                   new RedisConnectionLimiter(settings.MaxCount);
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
                ValidateNotDisposed();
                var connectionLimiter = m_ConnectionLimiter;
                return (connectionLimiter != null) ? connectionLimiter.AvailableCount : 0;
            }
        }

        public virtual int InUseCount
        {
            get
            {
                ValidateNotDisposed();
                var connectionLimiter = m_ConnectionLimiter;
                return (connectionLimiter != null) ? connectionLimiter.InUseCount : 0;
            }
        }

        public virtual int SpareCount { get { return 0; } }

        public string Name
        {
            get { return m_Name; }
        }

        #endregion Properties

        #region Methods

        #region IRedisConnectionProvider Methods

        protected virtual RedisSettings GetSettings()
        {
            return m_Settings;
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

        IRedisConnection IRedisConnectionProvider.Connect(int dbIndex)
        {
            return this.Connect(dbIndex);
        }
        
        protected internal virtual IRedisConnection Connect(int dbIndex)
        {
            ValidateNotDisposed();

            var settings = (GetSettings() ?? RedisSettings.Default);

            var spinStepTimeoutMs = GetConnectionSpinStepTimeout();

            var connectionTimeout = settings.ConnectionTimeout;
            connectionTimeout = connectionTimeout <= 0 ? RedisConstants.MaxConnectionTimeout : connectionTimeout;

            var retryInfo = new RedisConnectionRetryEventArgs((int)Math.Ceiling((double)settings.WaitTimeout / spinStepTimeoutMs),
                spinStepTimeoutMs, connectionTimeout, connectionTimeout);

            var limiterWait = (settings.MaxCount < 2) ? 0 : retryInfo.SpinStepTimeoutMs;

            while (retryInfo.RemainingTime > 0)
            {
                var signaled = m_ConnectionLimiter.Wait(limiterWait);
                if (signaled)
                    return NewConnection(DequeueSocket(dbIndex), dbIndex, true);

                retryInfo.Entered();
                OnConnectionRetry(retryInfo);

                if (!retryInfo.ContinueToSpin ||
                    retryInfo.CurrentRetryCount >= retryInfo.RetryCountLimit)
                {
                    OnConnectionLimitExceed(retryInfo);
                    if (retryInfo.ThrowError)
                        throw new RedisException("Wait retry count exited the given maximum limit");
                    return null;
                }
            }

            OnConnectionTimeout(retryInfo);
            if (retryInfo.ThrowError)
                throw new RedisException("Connection timeout occured while trying to connect");
            return null;
        }

        protected virtual IRedisConnection NewConnection(RedisSocket socket, int dbIndex, bool connectImmediately = true)
        {
            return null;
        }

        protected virtual RedisSocket DequeueSocket(int dbIndex)
        {
            return null;
        }

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

        #endregion IRedisConnectionProvider Methods

        #region IRedisCommandExecuter Methods

        protected internal virtual RedisResponse Execute(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.Execute(connection, throwException);
            }
        }

        protected internal virtual RedisRaw ExpectArray(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectArray(connection, throwException);
            }
        }

        protected internal virtual RedisString ExpectBulkString(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectBulkString(connection, throwException);
            }
        }

        protected internal virtual RedisBytes ExpectBulkStringBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectBulkStringBytes(connection, throwException);
            }
        }

        protected internal virtual RedisDouble ExpectDouble(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectDouble(connection, throwException);
            }
        }

        protected internal virtual RedisBool ExpectGreaterThanZero(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectInteger(connection, throwException) > RedisConstants.Zero;
            }
        }

        protected internal virtual RedisInteger ExpectInteger(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectInteger(connection, throwException);
            }
        }

        protected internal virtual RedisMultiBytes ExpectMultiDataBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectMultiDataBytes(connection, throwException);
            }
        }

        protected internal virtual RedisMultiString ExpectMultiDataStrings(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectMultiDataStrings(connection, throwException);
            }
        }

        protected internal virtual RedisVoid ExpectNothing(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectNothing(connection, throwException);
            }
        }

        protected internal virtual RedisNullableDouble ExpectNullableDouble(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectNullableDouble(connection, throwException);
            }
        }

        protected internal virtual RedisNullableInteger ExpectNullableInteger(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectNullableInteger(connection, throwException);
            }
        }

        protected internal virtual RedisBool ExpectOK(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectSimpleString(connection, "OK", throwException);
            }
        }

        protected internal virtual RedisBool ExpectOne(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectInteger(connection, throwException) == RedisConstants.One;
            }
        }

        protected internal virtual RedisBool ExpectSimpleString(RedisCommand command, string expectedResult, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectSimpleString(connection, expectedResult, throwException);
            }
        }

        protected internal virtual RedisString ExpectSimpleString(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectSimpleString(connection, throwException);
            }
        }

        protected internal virtual RedisBool ExpectSimpleStringBytes(RedisCommand command, byte[] expectedResult, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectSimpleStringBytes(connection, expectedResult, throwException);
            }
        }

        protected internal virtual RedisBytes ExpectSimpleStringBytes(RedisCommand command, bool throwException = true)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            ValidateNotDisposed();

            using (var connection = Connect(command.DbIndex))
            {
                return command.ExpectSimpleStringBytes(connection, throwException);
            }
        }

        #endregion IRedisCommandExecuter Methods

        #region IRedisCommandExecuter Methods

        RedisResponse IRedisCommandExecuter.Execute(RedisCommand command, bool throwException)
        {
            return this.Execute(command, throwException);
        }

        RedisRaw IRedisCommandExecuter.ExpectArray(RedisCommand command, bool throwException)
        {
            return this.ExpectArray(command, throwException);
        }

        RedisString IRedisCommandExecuter.ExpectBulkString(RedisCommand command, bool throwException)
        {
            return this.ExpectBulkString(command, throwException);
        }

        RedisBytes IRedisCommandExecuter.ExpectBulkStringBytes(RedisCommand command, bool throwException)
        {
            return this.ExpectBulkStringBytes(command, throwException);
        }

        RedisDouble IRedisCommandExecuter.ExpectDouble(RedisCommand command, bool throwException)
        {
            return this.ExpectDouble(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectGreaterThanZero(RedisCommand command, bool throwException)
        {
            return this.ExpectGreaterThanZero(command, throwException);
        }

        RedisInteger IRedisCommandExecuter.ExpectInteger(RedisCommand command, bool throwException)
        {
            return this.ExpectInteger(command, throwException);
        }

        RedisMultiBytes IRedisCommandExecuter.ExpectMultiDataBytes(RedisCommand command, bool throwException)
        {
            return this.ExpectMultiDataBytes(command, throwException);
        }

        RedisMultiString IRedisCommandExecuter.ExpectMultiDataStrings(RedisCommand command, bool throwException)
        {
            return this.ExpectMultiDataStrings(command, throwException);
        }

        RedisVoid IRedisCommandExecuter.ExpectNothing(RedisCommand command, bool throwException)
        {
            return this.ExpectNothing(command, throwException);
        }

        RedisNullableDouble IRedisCommandExecuter.ExpectNullableDouble(RedisCommand command, bool throwException)
        {
            return this.ExpectNullableDouble(command, throwException);
        }

        RedisNullableInteger IRedisCommandExecuter.ExpectNullableInteger(RedisCommand command, bool throwException)
        {
            return this.ExpectNullableInteger(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectOK(RedisCommand command, bool throwException)
        {
            return this.ExpectOK(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectOne(RedisCommand command, bool throwException)
        {
            return this.ExpectOne(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectSimpleString(RedisCommand command, string expectedResult, bool throwException)
        {
            return this.ExpectSimpleString(command, expectedResult, throwException);
        }

        RedisString IRedisCommandExecuter.ExpectSimpleString(RedisCommand command, bool throwException)
        {
            return this.ExpectSimpleString(command, throwException);
        }

        RedisBool IRedisCommandExecuter.ExpectSimpleStringBytes(RedisCommand command, byte[] expectedResult, bool throwException)
        {
            return this.ExpectSimpleStringBytes(command, expectedResult, throwException);
        }

        RedisBytes IRedisCommandExecuter.ExpectSimpleStringBytes(RedisCommand command, bool throwException)
        {
            return this.ExpectSimpleStringBytes(command, throwException);
        }

        #endregion IRedisCommandExecuter Methods

        #endregion Methods
    }
}
