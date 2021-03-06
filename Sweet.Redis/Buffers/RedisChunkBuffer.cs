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
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisChunkBuffer : RedisDisposable, IRedisWriter
    {
        #region Constants

        private const long One = RedisConstants.One;
        private const long Beginning = RedisConstants.Zero;

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

        public RedisChunkBuffer(int bufferSize = 0)
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

        public bool UnderlyingDisposed
        {
            get { return false; }
        }

        #endregion Properties

        #region Methods

        public void Flush()
        { }

        public int Write(char val)
        {
            return Write(Encoding.UTF8.GetBytes(new char[] { val }));
        }

        public int Write(short val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(int val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(long val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(ushort val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(uint val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(ulong val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(decimal val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(double val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(float val)
        {
            return Write(Encoding.UTF8.GetBytes(val.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(DateTime val)
        {
            return Write(Encoding.UTF8.GetBytes(val.Ticks.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(TimeSpan val)
        {
            return Write(Encoding.UTF8.GetBytes(val.Ticks.ToString(RedisConstants.InvariantCulture)));
        }

        public int Write(byte val)
        {
            int currPosition;
            var chunk = GetOutChunk(out currPosition);

            chunk[currPosition] = val;

            Interlocked.Add(ref m_Length, One);
            Interlocked.Add(ref m_Position, One);

            return 1;
        }

        public int Write(string val)
        {
            if (!val.IsEmpty())
                return Write(Encoding.UTF8.GetBytes(val));
            return 0;
        }

        public int Write(byte[] data)
        {
            if (data != null)
            {
                var dataLength = data.Length;
                if (dataLength == 1)
                    return Write(data[0]);
                if (dataLength > 0)
                    return Write(data, 0, data.Length);
            }
            return 0;
        }

        public int Write(byte[] data, int index, int length)
        {
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

                    return dataLength;
                }
            }
            return 0;
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
                Interlocked.Exchange(ref m_Position, Beginning);
            }
            return chunk;
        }

        public void Clear()
        {
            ClearInternal();
        }

        private void ClearInternal()
        {
            Interlocked.Exchange(ref m_Length, Beginning);
            Interlocked.Exchange(ref m_Position, Beginning);

            var chunks = Interlocked.Exchange(ref m_Chunks, null);
            if (chunks != null)
                chunks.Clear();
        }

        public byte[] ReleaseBuffer()
        {
            var currLength = Interlocked.Exchange(ref m_Length, Beginning);
            var currPosition = Interlocked.Exchange(ref m_Position, Beginning);

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
