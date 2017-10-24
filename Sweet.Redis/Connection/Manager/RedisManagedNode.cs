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

using System.Threading;

namespace Sweet.Redis
{
    internal class RedisManagedNode : RedisDisposable
    {
        #region Field Members

        private bool m_OwnsPool;
        private RedisEndPoint m_EndPoint;
        private RedisManagedConnectionPool m_Pool;

        #endregion Field Members

        #region .Ctors

        public RedisManagedNode(RedisRole role, RedisManagedConnectionPool pool, bool ownsPool = true)
        {
            Role = role;
            m_Pool = pool;
            m_OwnsPool = ownsPool;
            m_EndPoint = pool.Settings.EndPoints[0];
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            var pool = Interlocked.Exchange(ref m_Pool, null);
            if (m_OwnsPool && pool != null)
                pool.Dispose();
        }

        #endregion Destructors

        #region Properties

        public RedisEndPoint EndPoint { get { return m_EndPoint; } }

        public bool OwnsPool { get { return m_OwnsPool; } }

        public RedisManagedConnectionPool Pool { get { return m_Pool; } }

        public RedisRole Role { get; internal set; }

        #endregion Properties

        #region Methods

        public RedisManagedNodeInfo GetNodeInfo()
        {
            return new RedisManagedNodeInfo(m_EndPoint, Role);
        }

        public RedisConnectionPool ExchangePool(RedisManagedConnectionPool pool)
        {
            ValidateNotDisposed();
            return Interlocked.Exchange(ref m_Pool, pool);
        }


        #endregion Methods
    }
}
