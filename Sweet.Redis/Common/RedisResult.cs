using System;
namespace Sweet.Redis
{
    public class RedisResult<T>
    {
        #region Field Members

        private T m_Value;

        #endregion Field Members

        #region Properties

        public bool IsCompleted { get; protected set; }

        public object RawData { get { return m_Value; } }

        public virtual RedisResultType Type { get; }

        public virtual T Value
        {
            get
            {
                ValidateCompleted();
                return default(T);
            }
            internal set
            {
                m_Value = value;
                IsCompleted = true;
            }
        }

        #endregion Properties

        #region Methods

        protected virtual void ValidateCompleted()
        {
            if (!IsCompleted)
                throw new RedisException("Result is not completed");
        }

        #endregion Methods
    }
}
