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
    public class RedisConnectionProvider : RedisDisposable
    {
        #region Constants

        protected const int SpinStepMilliseconds = 20;

        #endregion Constants

        #region Field Members

        private string m_Name;
        private RedisSettings m_Settings;
        private RedisConnectionLimiter m_ConnectionLimiter;

        #endregion Field Members

        #region .Ctors

        public RedisConnectionProvider(string name)
            : this(name, RedisSettings.Default)
        { }

        public RedisConnectionProvider(string name, RedisSettings settings)
        {
            if (settings == null)
                throw new ArgumentNullException("settings");

            m_Settings = settings ?? RedisSettings.Default;

            name = (name ?? String.Empty).Trim();
            m_Name = !String.IsNullOrEmpty(name) ? name : Guid.NewGuid().ToString("N").ToUpper();

            m_ConnectionLimiter = CreateConnectionLimiter();
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

        public int InUseCount
        {
            get
            {
                ValidateNotDisposed();
                var connectionLimiter = m_ConnectionLimiter;
                return (connectionLimiter != null) ? connectionLimiter.InUseCount : 0;
            }
        }

        public string Name
        {
            get { return m_Name; }
        }

        #endregion Properties

        #region Methods

        protected virtual RedisSettings GetSettings()
        {
            return m_Settings;
        }

        protected virtual RedisConnectionLimiter CreateConnectionLimiter()
        {
            var settings = GetSettings() ?? RedisSettings.Default;
            return new RedisConnectionLimiter(settings.MaxCount);
        }

        protected virtual int GetWaitRetryCount()
        {
            var settings = (GetSettings() ?? RedisSettings.Default);
            return (int)Math.Ceiling((double)settings.WaitTimeout / SpinStepMilliseconds);
        }

        protected virtual void OnConnectionLimitExceed(out bool throwError)
        {
            throwError = true;
        }

        protected virtual void OnConnectionTimeout(out bool throwError)
        {
            throwError = true;
        }

        internal virtual IRedisConnection Connect(int db = -1)
        {
            ValidateNotDisposed();

            var settings = (GetSettings() ?? RedisSettings.Default);

            var timeout = settings.ConnectionTimeout;
            timeout = timeout <= 0 ? RedisConstants.MaxConnectionTimeout : timeout;

            var now = DateTime.UtcNow;

            var retryCount = 0;
            var remainingTime = timeout;
            var waitRetryCount = GetWaitRetryCount();

            while (remainingTime > 0)
            {
                var signaled = m_ConnectionLimiter.Wait(SpinStepMilliseconds);
                if (signaled)
                    return NewConnection(Dequeue(db), db, true);

                retryCount++;
                remainingTime = timeout - (int)(DateTime.UtcNow - now).TotalMilliseconds;

                if (retryCount >= waitRetryCount)
                {
                    bool throwLimitError;
                    OnConnectionLimitExceed(out throwLimitError);
                    if (throwLimitError)
                        throw new RedisException("Wait retry count exited the given maximum limit");
                    return null;
                }
            }

            bool throwTimeoutError;
            OnConnectionTimeout(out throwTimeoutError);
            if (throwTimeoutError)
                throw new RedisException("Connection timeout occured while trying to connect");
            return null;
        }

        protected virtual IRedisConnection NewConnection(RedisSocket socket, int db, bool connectImmediately = true)
        {
            return null;
        }

        protected virtual RedisSocket Dequeue(int db)
        {
            return null;
        }

        protected void Release()
        {
            var connectionLimiter = m_ConnectionLimiter;
            if (connectionLimiter != null)
                connectionLimiter.Release();
        }

        protected virtual void OnRelease(IRedisConnection conn, RedisSocket socket)
        {
            ValidateNotDisposed();
            try
            {
                CompleteRelease(conn, socket);
            }
            finally
            {
                Release();
            }
        }

        protected virtual void CompleteRelease(IRedisConnection conn, RedisSocket socket)
        {
        }

        #endregion Methods
    }
}
