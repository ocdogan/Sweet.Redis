using System;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisCommandSet : RedisDisposable
    {
        #region Field Members

        private IRedisDb m_Db;
        private Guid m_Id;

        #endregion Field Members

        #region .Ctors

        public RedisCommandSet(IRedisDb db)
        {
            m_Db = db;
            m_Id = db.Id;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Db, null);
        }

        #endregion Destructors

        #region Properties

        public IRedisDb Db
        {
            get { return m_Db; }
        }

        public Guid Id
        {
            get { return m_Id; }
        }

        #endregion Properties

        #region Methods

        public override void ValidateNotDisposed()
        {
            if (Disposed)
                throw new ObjectDisposedException(GetType().Name + ", " + m_Id.ToString("N"));
        }

        protected static void ValidateKeyAndValue(string key, byte[] value, string keyName = null, string valueName = null)
        {
            if (key == null)
                throw new ArgumentNullException(String.IsNullOrEmpty(keyName) ? "key" : keyName);

            if (value == null)
                throw new ArgumentNullException(String.IsNullOrEmpty(valueName) ? "value" : valueName);

            if (value.Length > RedisConstants.MaxValueLength)
                throw new ArgumentException("Redis values are limited to 1GB", String.IsNullOrEmpty(valueName) ? "value" : valueName);
        }

        #endregion Methods
    }
}
