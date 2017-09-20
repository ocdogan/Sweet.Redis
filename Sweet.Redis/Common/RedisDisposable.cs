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
    public class RedisDisposable : IRedisDisposable
    {
        #region Field Members

        private long m_Disposed;

        #endregion Field Members

        #region Destructors

        ~RedisDisposable()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            var alreadyDisposed = SetDisposed();
            try
            {
                OnBeforeDispose(disposing, alreadyDisposed);
            }
            finally
            {
                if (!alreadyDisposed)
                {
                    if (disposing)
                        GC.SuppressFinalize(this);

                    OnDispose(disposing);
                }
            }
        }

        protected virtual void OnBeforeDispose(bool disposing, bool alreadyDisposed)
        { }

        protected virtual void OnDispose(bool disposing)
        { }

        #endregion Destructors

        #region Properties

        public bool Disposed
        {
            get { return Interlocked.Read(ref m_Disposed) != 0; }
        }

        #endregion Properties

        #region Methods

        protected virtual bool SetDisposed()
        {
            return Interlocked.Exchange(ref m_Disposed, RedisConstants.True) != RedisConstants.False;
        }

        public virtual void ValidateNotDisposed()
        {
            if (Disposed)
                throw new RedisException(GetType().Name + " is disposed");
        }

        #endregion Methods
    }
}
