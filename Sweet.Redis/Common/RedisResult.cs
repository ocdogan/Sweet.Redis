using System;
namespace Sweet.Redis
{
    public class RedisResult<T>
    {
        #region Field Members

        private T m_Value;
        private RedisResultStatus m_Status = RedisResultStatus.Pending;

        #endregion Field Members

        #region .Ctors

        internal RedisResult()
        { }

        internal RedisResult(T value)
        {
            m_Value = value;
            m_Status = RedisResultStatus.Completed;
        }

        #endregion .Ctors

        #region Properties

        public bool IsCompleted
        {
            get { return m_Status == RedisResultStatus.Completed; }
            protected set
            {
                m_Status = value ? RedisResultStatus.Completed : RedisResultStatus.Pending;
            }
        }

        public object RawData { get { return m_Value; } }

        public virtual RedisResultType Type { get { return RedisResultType.Undefined; } }

        public RedisResultStatus Status
        {
            get { return m_Status; }
            internal set { m_Status = value; }
        }

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
