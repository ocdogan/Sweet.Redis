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
            if (SetDisposed())
                return;

            if (disposing)
                GC.SuppressFinalize(this);

            OnDispose(disposing);
        }

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
            return Interlocked.Exchange(ref m_Disposed, 1L) != 0L;
        }

        public virtual void ValidateNotDisposed()
        {
            if (Disposed)
                throw new RedisException(GetType().Name + " is disposed");
        }

        #endregion Methods
    }
}
