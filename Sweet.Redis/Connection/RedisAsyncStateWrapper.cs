using System;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisAsyncStateWrapper : RedisDisposable
    {
        #region Field Members

        private object m_Tag;
        private object m_RealState;

        #endregion Field Members

        #region .Ctors

        public RedisAsyncStateWrapper(object realState, object tag = null)
        {
            m_Tag = tag;
            m_RealState = realState;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Tag, null);
            Interlocked.Exchange(ref m_RealState, null);

            base.OnDispose(disposing);
        }

        #endregion Destructors

        #region Properties

        public object RealState
        {
            get { return m_RealState; }
        }

        public object Tag
        {
            get { return m_Tag; }
        }

        #endregion Properties
    }
}
