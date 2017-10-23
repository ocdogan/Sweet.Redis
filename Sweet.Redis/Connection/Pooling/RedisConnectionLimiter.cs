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
    public class RedisConnectionLimiter : RedisDisposable
    {
        #region Field Members

        private int m_MaxCount;
        private SemaphoreSlim m_CountSync;
        private readonly object m_SyncLock = new object();

        #endregion Field Members
        
        #region .Ctors

        public RedisConnectionLimiter(int maxCount)
        {
            m_MaxCount = Math.Max(Math.Min(maxCount, RedisConstants.MaxConnectionCount), RedisConstants.MinConnectionCount);
            m_CountSync = new SemaphoreSlim(m_MaxCount, m_MaxCount);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            lock (m_SyncLock)
            {
                var countSync = Interlocked.Exchange(ref m_CountSync, null);
                if (countSync != null)
                {
                    try
                    {
                        countSync.Dispose();
                    }
                    catch (Exception)
                    { }
                }
            }
        }

        #endregion Destructors

        #region Properties

        public int AvailableCount
        {
            get
            {
                var countSync = m_CountSync;
                return (countSync != null) ? countSync.CurrentCount : 0;
            }
        }

        public int InUseCount
        {
            get
            {
                var countSync = m_CountSync;
                return (countSync != null) ? m_MaxCount - countSync.CurrentCount : 0;
            }
        }

        #endregion Properties

        #region Methods

        public bool Wait(int timeout = Timeout.Infinite)
        {
            return m_CountSync.Wait(Math.Max(Timeout.Infinite, timeout));
        }

        public int Release()
        {
            lock (m_SyncLock)
            {
                var count = m_CountSync.CurrentCount;
                if (count < m_MaxCount)
                {
                    count = m_CountSync.Release();
                }
                return (int)count;
            }
        }

        #endregion Methods
    }
}
