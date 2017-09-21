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
    public class RedisConnectionProvider : RedisDisposable
    {
        #region Field Members

        private long m_Count;
        private string m_Name;
        private RedisSettings m_Settings;
        private Semaphore m_MaxConnectionCountSync;

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

            m_MaxConnectionCountSync = CreateConnectionLimiter();
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            var countSync = Interlocked.Exchange(ref m_MaxConnectionCountSync, null);
            if (countSync != null)
                countSync.Close();
        }

        #endregion Destructors

        #region Properties

        public long Count
        {
            get { return Interlocked.Read(ref m_Count); }
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

        protected virtual Semaphore CreateConnectionLimiter()
        {
            var settings = GetSettings() ?? RedisSettings.Default;
            return new Semaphore(settings.MaxCount, settings.MaxCount);
        }

        internal virtual IRedisConnection Connect(int db = -1)
        {
            ValidateNotDisposed();

            var settings = (GetSettings() ?? RedisSettings.Default);

            var timeout = settings.ConnectionTimeout;
            timeout = timeout <= 0 ? RedisConstants.MaxConnectionTimeout : timeout;

            var now = DateTime.UtcNow;
            var remainingTime = timeout;

            var retryCount = 0;

            while (remainingTime > 0)
            {
                var signaled = m_MaxConnectionCountSync.WaitOne(settings.WaitTimeout);
                if (!signaled)
                {
                    retryCount++;
                    if (retryCount > settings.WaitRetryCount)
                        throw new RedisException("Wait retry count exited the given maximum limit");
                }

                var socket = Dequeue(db);

                if ((socket != null) ||
                    (Interlocked.Read(ref m_Count) < settings.MaxCount))
                {
                    IRedisConnection conn = null;
                    try
                    {
                        conn = NewConnection(socket, db, true);
                        return conn;
                    }
                    finally
                    {
                        if (conn != null)
                            IncrementCount();
                    }
                }

                remainingTime = timeout - (int)(DateTime.UtcNow - now).TotalMilliseconds;
            }
            throw new RedisException("Connection timeout occured while trying to connect");
        }

        protected virtual IRedisConnection NewConnection(RedisSocket socket, int db, bool connectImmediately = true)
        {
            return null;
        }

        protected virtual RedisSocket Dequeue(int db)
        {
            return null;
        }

        protected void IncrementCount()
        {
            Interlocked.Increment(ref m_Count);
        }

        protected virtual void OnRelease(IRedisConnection conn, RedisSocket socket)
        {
            ValidateNotDisposed();

            if (Interlocked.Read(ref m_Count) > RedisConstants.Zero)
            {
                try
                {
                    Interlocked.Decrement(ref m_Count);
                    CompleteRelease(conn, socket);
                }
                finally
                {
                    m_MaxConnectionCountSync.Release();
                }
            }
        }

        protected virtual void CompleteRelease(IRedisConnection conn, RedisSocket socket)
        {
        }

        #endregion Methods
    }
}
