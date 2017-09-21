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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal class RedisAsyncRequest : IRedisDisposable
    {
        #region Field Members

        private long m_Disposed;

        private RedisCommand m_Command;
        private TaskCompletionSource<IRedisResponse> m_CompletionSource;

        #endregion Field Members

        #region .Ctors

        public RedisAsyncRequest(RedisCommand command, TaskCompletionSource<IRedisResponse> completionSource)
        {
            m_Command = command;
            m_CompletionSource = completionSource;
        }

        #endregion .Ctors

        #region Destructors

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref m_Disposed, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                Interlocked.Exchange(ref m_Command, null);
                Interlocked.Exchange(ref m_CompletionSource, null);
            }
        }

        #endregion Destructors

        #region Properties

        public RedisCommand Command
        {
            get { return m_Command; }
        }

        public bool IsCompleted
        {
            get
            {
                var tcs = m_CompletionSource;
                if (tcs != null)
                {
                    var task = tcs.Task;
                    return (task == null) || task.IsCompleted;
                }
                return true;
            }
        }

        public TaskCompletionSource<IRedisResponse> CompletionSource
        {
            get { return m_CompletionSource; }
        }

        public bool Disposed
        {
            get { return Interlocked.Read(ref m_Disposed) != RedisConstants.Zero; }
        }

        public Task<IRedisResponse> Task
        {
            get
            {
                var tcs = m_CompletionSource;
                if (tcs != null)
                    return tcs.Task;
                return null;
            }
        }

        #endregion Properties

        #region Methods

        public virtual void ValidateNotDisposed()
        {
            if (Disposed)
                throw new RedisException(GetType().Name + " is disposed");
        }

        #endregion Methods
    }
}
