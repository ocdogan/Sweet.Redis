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
    internal class RedisContinuousConnectionProvider : RedisSingleConnectionProvider
    {
        #region Field Members

        private Action<IRedisRawResponse> m_OnReceiveResponse;

        #endregion Field Members

        #region .Ctors

        public RedisContinuousConnectionProvider(string name, RedisPoolSettings settings, Action<IRedisRawResponse> onReceiveResponse)
            : base(name, settings)
        {
            m_OnReceiveResponse = onReceiveResponse;
        }

        #endregion .Ctors

        #region Methods

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            Interlocked.Exchange(ref m_OnReceiveResponse, null);
        }

        protected internal override IRedisConnection Connect(int dbIndex, RedisRole expectedRole)
        {
            ValidateNotDisposed();

            var connection = m_Connection;
            if (connection.IsAlive())
                return connection;

            return base.Connect(dbIndex, expectedRole);
        }

        protected override IRedisConnection OnNewConnection(RedisSocket socket, int dbIndex, RedisRole role, bool connectImmediately = true)
        {
            var settings = (Settings as RedisPoolSettings) ?? RedisPoolSettings.Default;
            return new RedisContinuousReaderConnection(Name, RedisRole.Master, settings,
                OnReceiveResponse,
                null,
                OnReleaseSocket,
                socket,
                true);
        }

        protected virtual void OnReceiveResponse(IRedisRawResponse response)
        {
            if (!Disposed)
            {
                var onReceiveResponse = m_OnReceiveResponse;
                if (onReceiveResponse != null)
                    onReceiveResponse(response);
            }
        }

        #endregion Methods
    }
}
