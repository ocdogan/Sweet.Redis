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
    public class RedisMonitorChannel : RedisContinuousChannel<RedisMonitorMessage>, IRedisMonitorChannel
    {
        #region Field Members

        private Action<object> m_OnComplete;

        #endregion Field Members

        #region .Ctors

        internal RedisMonitorChannel(RedisPoolSettings settings, Action<object> onComplete)
            : base(settings)
        {
            m_OnComplete = onComplete;
            RegisterOnComplete(OnComplete);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_OnComplete, null);
            base.OnDispose(disposing);
        }

        #endregion Destructors

        #region Methods

        private void OnComplete(object sender)
        {
            var onComplete = m_OnComplete;
            if (onComplete != null)
                onComplete(this);
        }

        protected override bool CanBeginReceive(byte[] cmd)
        {
            return cmd == RedisCommandList.Monitor;
        }

        protected override bool TryConvertResponse(IRedisRawResponse response, out RedisMonitorMessage value)
        {
            value = RedisMonitorMessage.ToMonitorMessage(response);
            return !value.IsEmpty;
        }

        protected override void OnSubscribe()
        {
            SendAsync(RedisCommandList.Monitor);
        }

        public void Monitor()
        {
            ValidateNotDisposed();
            SendAsync(RedisCommandList.Monitor);
        }

        #endregion Methods
    }
}
