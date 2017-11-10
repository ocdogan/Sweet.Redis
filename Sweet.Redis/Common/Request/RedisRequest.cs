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
    internal abstract class RedisRequest : IRedisDisposable
    {
        #region Field Members

        private long m_Disposed;
        private DateTime m_CreationTime;

        private long m_Id = RedisIDGenerator<RedisRequest>.NextId();
        private RedisRequestType m_RequestType;
        private string m_OKIf;
        private RedisCommandExpect m_Expectation;

        protected RedisCommand m_Command;
        protected object m_StateObject;

        #endregion Field Members

        #region .Ctors

        protected RedisRequest(RedisCommand command, RedisCommandExpect expectation,
            string okIf = null, object stateObject = null, RedisRequestType requestType = RedisRequestType.Default)
        {
            m_OKIf = okIf;
            m_Expectation = expectation;
            m_Command = command;
            m_StateObject = stateObject;
            m_RequestType = requestType;
            m_CreationTime = DateTime.UtcNow;
        }

        #endregion .Ctors

        #region Destructors

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref m_Disposed, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                var cancellationRequired = !(IsCompleted || IsCanceled || IsFaulted);

                Interlocked.Exchange(ref m_Command, null);
                Interlocked.Exchange(ref m_StateObject, null);

                if (cancellationRequired)
                    Cancel();
            }
        }

        #endregion Destructors

        #region Properties

        public RedisCommand Command
        {
            get { return m_Command; }
        }

        public DateTime CreationTime
        {
            get { return m_CreationTime; }
        }

        public bool Disposed
        {
            get { return Interlocked.Read(ref m_Disposed) != RedisConstants.Zero; }
        }

        public RedisCommandExpect Expectation
        {
            get { return m_Expectation; }
        }

        public long Id { get { return m_Id; } }

        public virtual bool IsAsync
        {
            get { return RequestType == RedisRequestType.Async; }
        }

        public abstract bool IsCanceled { get; }

        public abstract bool IsCompleted { get; }

        public abstract bool IsFaulted { get; }

        public virtual bool IsPipelined
        {
            get { return RequestType == RedisRequestType.Pipelined; }
        }

        public abstract bool IsStarted { get; }

        public virtual bool IsTransactional
        {
            get { return RequestType == RedisRequestType.Transactional; }
        }

        public string OKIf
        {
            get { return m_OKIf; }
        }

        public virtual RedisRequestType RequestType
        {
            get { return m_RequestType; }
        }

        public object StateObject
        {
            get { return m_StateObject; }
        }

        #endregion Properties

        #region Methods

        public virtual void ValidateNotDisposed()
        {
            if (Disposed)
                throw new RedisException(GetType().Name + " is disposed", RedisErrorCode.ObjectDisposed);
        }

        public abstract void Cancel();

        public abstract void Process(RedisSocketContext context, int timeoutMilliseconds = -1);

        public abstract void SetException(Exception exception);

        public abstract void SetResult(object value);

        public abstract bool Send(RedisSocketContext context, int timeoutMilliseconds = -1);

        public abstract bool Receive(RedisSocketContext context, int timeoutMilliseconds = -1);

        #endregion Methods
    }
}
