using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisDataBuffer : RedisDisposable
    {
        #region Constants

        private const int MinBufferSize = 512;
        private const int MaxBufferSize = 16 * 1024;
        private const int DefaultBufferSize = 4 * 1024;

        #endregion Constants

        #region Field Members

        private long m_Length;
        private long m_Position;

        private readonly int m_ChunkSize;
        private List<byte[]> m_Chunks = new List<byte[]>();

        #endregion Field Members

        #region .Ctors

        public RedisDataBuffer(int bufferSize = 0)
        {
            m_ChunkSize = (bufferSize <= 0) ? DefaultBufferSize : 
                Math.Min(MaxBufferSize, Math.Max(MinBufferSize, bufferSize));
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            ClearInternal();
        }

        #endregion Destructors

        #region Properties

        public int Length
        {
            get { return (int)Interlocked.Read(ref m_Length); }
        }

        public int Position
        {
            get { return (int)Interlocked.Read(ref m_Position); }
            set
            {
                Interlocked.Exchange(ref m_Position,
                                     Math.Min(Math.Max(0, value),
                                              Interlocked.Read(ref m_Length)));
            }
        }

        #endregion Properties

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

            int currPosition;
            var chunk = GetOutChunk(out currPosition);

            chunk[currPosition] = val;

            Interlocked.Add(ref m_Length, 1L);
            Interlocked.Add(ref m_Position, 1L);
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

                    int currPosition;
                    var chunk = GetOutChunk(out currPosition);

                    var copyLength = 0;
                    while ((copyLength = Math.Min(m_ChunkSize - currPosition, length)) > 0 && length > 0)
                    {
                        Buffer.BlockCopy(data, index, chunk, currPosition, copyLength);

                        index += copyLength;
                        length -= copyLength;

                        Interlocked.Add(ref m_Length, copyLength);
                        Interlocked.Add(ref m_Position, copyLength);

                        if (length > 0)
                            chunk = GetOutChunk(out currPosition);
                    }
                }
            }
        }

        protected List<byte[]> GetChunks()
        {
            var chunks = m_Chunks;
            if (chunks == null)
            {
                chunks = new List<byte[]>();
                Interlocked.Exchange(ref m_Chunks, chunks);
            }
            return chunks;
        }

        protected byte[] GetOutChunk(out int position)
        {
            position = (int)Interlocked.Read(ref m_Position);

            var chunks = GetChunks();

            var chunk = (chunks.Count > 0) ? chunks[chunks.Count - 1] : null;
            if (chunk == null || (position >= m_ChunkSize))
            {
                chunk = new byte[m_ChunkSize];
                chunks.Add(chunk);

                position = 0;
                Interlocked.Exchange(ref m_Position, 0L);
            }
            return chunk;
        }

        public void Clear()
        {
            ValidateNotDisposed();
            ClearInternal();
        }

        private void ClearInternal()
        {
            Interlocked.Exchange(ref m_Length, 0);
            Interlocked.Exchange(ref m_Position, 0);

            var chunks = Interlocked.Exchange(ref m_Chunks, null);
            if (chunks != null)
                chunks.Clear();
        }

        public byte[] ReleaseBuffer()
        {
            ValidateNotDisposed();

            var currLength = Interlocked.Exchange(ref m_Length, 0L);
            var currPosition = Interlocked.Exchange(ref m_Position, 0L);

            var chunks = Interlocked.Exchange(ref m_Chunks, null);
            if (chunks == null)
                return null;

            var chunkCount = chunks.Count;
            if (chunkCount == 0)
                return null;

            var result = new byte[currLength];

            var index = 0;
            for (var i = 0; i < chunkCount && currLength > 0; i++)
            {
                var chunk = chunks[i];

                var chunkLength = (int)Math.Min(m_ChunkSize, currLength);
                currLength -= chunkLength;

                if (chunkLength > 0)
                {
                    Buffer.BlockCopy(chunk, 0, result, index, chunkLength);
                    index += chunkLength;
                }
            }

            chunks.Clear();

            return result;
        }

        #endregion Methods
    }
}
