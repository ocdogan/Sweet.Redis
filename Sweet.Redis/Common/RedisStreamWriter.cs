using System;
using System.IO;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisStreamWriter : RedisDisposable, IRedisWriter
    {
        #region Field Members

        private Stream m_Stream;
        private bool m_OwnsStream;

        #endregion Field Members

        #region .Ctors

        public RedisStreamWriter(Stream stream, bool ownsStream = false)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            m_Stream = stream;
            m_OwnsStream = ownsStream;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            var stream = Interlocked.Exchange(ref m_Stream, null);
            if (m_OwnsStream && stream != null)
                stream.Dispose();
        }

        #endregion Destructors

        #region Methods

        public void Write(char val)
        {
            Write(Encoding.UTF8.GetBytes(new char[] { val }));
        }

        public void Write(short val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(int val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(long val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(ushort val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(uint val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(ulong val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(decimal val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(double val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(float val)
        {
            Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(DateTime val)
        {
            Write(Encoding.UTF8.GetBytes(val.Ticks.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(TimeSpan val)
        {
            Write(Encoding.UTF8.GetBytes(val.Ticks.ToString(RedisConstants.InvariantCulture)));
        }

        public void Write(string val)
        {
            if (!String.IsNullOrEmpty(val))
                Write(Encoding.UTF8.GetBytes(val));
        }

        public void Write(byte[] data)
        {
            if (data != null)
            {
                var dataLength = data.Length;
                if (dataLength == 1)
                    Write(data[0]);
                else if (dataLength > 0)
                    Write(data, 0, data.Length);
            }
        }

        public void Write(byte val)
        {
            ValidateNotDisposed();
            m_Stream.Write(new byte[] { val }, 0, 1);
        }

        public void Write(byte[] data, int index, int length)
        {
            ValidateNotDisposed();

            if (index < 0)
                throw new ArgumentException("Index value is out of bounds", "index");

            if (length <= 0)
                throw new ArgumentException("Length can not be less than or equal to zero", "length");

            if (data != null)
            {
                var dataLength = data.Length;
                if (dataLength > 0)
                {
                    if (index + length > dataLength)
                        throw new ArgumentException("Length can not exceed data size", "length");

                    if (dataLength < 512)
                        m_Stream.Write(data, index, length);
                    else
                        m_Stream.WriteAsync(data, index, length).Wait();
                }
            }
        }

        #endregion Methods
    }
}
