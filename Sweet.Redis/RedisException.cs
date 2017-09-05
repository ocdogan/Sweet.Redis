using System;
using System.Runtime.Serialization;

namespace Sweet.Redis
{
    [Serializable]
    public class RedisException : Exception
    {
        private string m_Prefix;

        public RedisException()
            : base()
        { }

        public RedisException(string message)
            : base(message)
        { }

        public RedisException(string prefix, string message)
            : base(message)
        {
            Prefix = prefix;
        }

        public RedisException(string message, Exception innerException)
            : base(message, innerException)
        { }

        public RedisException(string prefix, string message, Exception innerException)
            : base(message, innerException)
        {
            Prefix = prefix;
        }

        public RedisException(string message, Exception innerException, params object[] args)
            : base(string.Format(message, args), innerException)
        { }

        public RedisException(string prefix, string message, Exception innerException, params object[] args)
            : base(string.Format(message, args), innerException)
        {
            Prefix = prefix;
        }

        protected RedisException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        { }

        public string Prefix
        {
            get { return String.IsNullOrEmpty(m_Prefix) ? "ERR" : m_Prefix; }
            set
            {
                if (value == null)
                {
                    m_Prefix = null;
                }
                else
                {
                    m_Prefix = value.Trim();
                    if (m_Prefix == String.Empty)
                        m_Prefix = null;
                }
            }
        }
    }
}
