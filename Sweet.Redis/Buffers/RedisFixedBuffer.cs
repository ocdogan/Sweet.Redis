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
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisFixedBuffer : RedisDisposable
    {
        #region Field Members

        private byte[] m_Data;
        private long m_Length;
        private long m_Capacity;
        private long m_Position;

        #endregion Field Members

        #region .Ctors

        public RedisFixedBuffer(int capacity)
        {
            m_Capacity = Math.Max(0, capacity);
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            ClearInternal();
        }

        #endregion Destructors

        #region Properties

        public int Capacity
        {
            get { return (int)Interlocked.Read(ref m_Capacity); }
        }

        public bool Completed
        {
            get
            {
                return Interlocked.Read(ref m_Position) >=
                              Interlocked.Read(ref m_Length);
            }
        }

        public byte[] Data
        {
            get { return m_Data; }
        }

        public int Length
        {
            get { return (int)Interlocked.Read(ref m_Length); }
        }

        public int Position
        {
            get { return (int)Interlocked.Read(ref m_Position); }
        }

        #endregion Properties

        #region Methods

        private void ValidateNotCompleted()
        {
            if (Completed)
                throw new RedisException("Buffer capacity exceeded");
        }

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
            ValidateNotCompleted();

            GetBuffer()[Position] = val;

            Interlocked.Add(ref m_Position, 1L);
        }

        private byte[] GetBuffer()
        {
            var buffer = m_Data;
            if (buffer == null)
            {
                var capacity = Capacity;
                buffer = new byte[capacity];

                Interlocked.Exchange(ref m_Data, buffer);

                Interlocked.Exchange(ref m_Position, 0L);
                Interlocked.Exchange(ref m_Length, capacity);
            }
            return buffer;
        }

        public void Write(byte[] data, int index, int length)
        {
            ValidateNotDisposed();
            ValidateNotCompleted();

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

                    var currPosition = Position;
                    if (length > Length - currPosition)
                        throw new ArgumentException("Length can not exceed buffer capacitye", "length");

                    var buffer = GetBuffer();

                    Buffer.BlockCopy(data, index, buffer, currPosition, length);

                    Interlocked.Add(ref m_Position, length);
                }
            }
        }

        public void Clear()
        {
            ValidateNotDisposed();
            ClearInternal();
        }

        private void ClearInternal()
        {
            Interlocked.Exchange(ref m_Length, 0L);
            Interlocked.Exchange(ref m_Position, 0L);

            Interlocked.Exchange(ref m_Data, null);
        }

        public int ReleaseBuffer(out byte[] data, bool reinit = false)
        {
            ValidateNotDisposed();

            data = m_Data;
            var pos = (int)Interlocked.Exchange(ref m_Position, 0L);

            if (reinit)
            {
                var capacity = Capacity;

                Interlocked.Exchange(ref m_Length, capacity);
                Interlocked.Exchange(ref m_Data, new byte[capacity]);
            }
            else
            {
                Interlocked.Exchange(ref m_Length, 0L);
                Interlocked.Exchange(ref m_Data, null);
            }

            return pos;
        }

        #endregion Methods
    }
}
