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

using System.Threading;

namespace Sweet.Redis
{
    internal class RedisCluster : RedisDisposable
    {
        #region Field Members

        private RedisManagedNodesGroup m_Masters; 
        private RedisManagedNodesGroup m_Slaves;
        private RedisManagedNodesGroup m_Sentinels;

        #endregion Field Members

        #region .Ctors

        public RedisCluster(RedisManagedNodesGroup masters, 
            RedisManagedNodesGroup slaves = null, RedisManagedNodesGroup sentinels = null)
        {
            m_Masters = masters;
            m_Slaves = slaves;
            m_Sentinels = sentinels;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            var masters = Interlocked.Exchange(ref m_Masters, null);
            var slaves = Interlocked.Exchange(ref m_Slaves, null);
            var sentinels = Interlocked.Exchange(ref m_Sentinels, null);

            if (masters != null) masters.Dispose();
            if (slaves != null) slaves.Dispose();
            if (sentinels != null) sentinels.Dispose();
        }

        #endregion Destructors

        #region Properties

        public RedisManagedNodesGroup Masters { get { return m_Masters; } }

        public RedisManagedNodesGroup Slaves { get { return m_Slaves; } }

        public RedisManagedNodesGroup Sentinels { get { return m_Sentinels; } } 

        #endregion Properties
    }
}
