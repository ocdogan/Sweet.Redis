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
    internal class RedisConnectionLimiter : RedisDisposable
    {
        #region Field Members

        private long m_Count;
        private int m_MaxCount; 
        private Semaphore m_CountSync;

        #endregion Field Members
        #region .Ctors

        public RedisConnectionLimiter(int maxCount)
        {
            m_MaxCount = Math.Max(1, maxCount);
            m_CountSync = new Semaphore(maxCount, maxCount);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            var countSync = Interlocked.Exchange(ref m_CountSync, null);
            if (countSync != null)
                countSync.Close();
        }

        #endregion Destructors

        #region Methods

        public bool WaitOne(int timeout = Timeout.Infinite)
        {
            var signalled = m_CountSync.WaitOne(Math.Max(Timeout.Infinite, timeout));
            if (signalled)
                Interlocked.Increment(ref m_Count);

            return signalled;
        }

        public int Release()
        {
            var count = Interlocked.Read(ref m_Count);
            if (count > RedisConstants.Zero)
            {
                var oldCount = m_CountSync.Release();
                if (oldCount != count)
                    Interlocked.Decrement(ref m_Count);
            }
            return (int)count;
        }

        #endregion Methods
    }
}
