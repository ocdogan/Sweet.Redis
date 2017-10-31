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

namespace Sweet.Redis
{
    public class RedisFixedBuffer : RedisDisposable
    {
        #region Constants

        public const int DefaultBufferSize = 1024;

        private const int Beginning = (int)RedisConstants.Zero;

        #endregion Constants

        #region Field Members

        private byte[] m_Buffer;
        private readonly int m_Capacity;

        private int m_ReadPosition;
        private int m_WritePosition;

        #endregion Field Members

        #region .Ctors

        public RedisFixedBuffer(int capacity)
        {
            capacity = Math.Max(0, capacity);
            m_Capacity = capacity == 0 ? DefaultBufferSize : capacity;

            m_Buffer = new byte[m_Capacity];
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Reset();
        }

        #endregion Destructors

        #region Properties

        public int Capacity
        {
            get { return m_Capacity; }
        }

        public bool Completed
        {
            get { return m_WritePosition >= m_Capacity; }
        }

        public byte[] Buffer
        {
            get { return m_Buffer; }
        }

        public int ReadPosition
        {
            get { return m_ReadPosition; }
        }

        public int WritePosition
        {
            get { return m_WritePosition; }
        }

        #endregion Properties

        #region Methods

        private void ValidateNotCompleted()
        {
            if (Completed)
                throw new RedisException("Buffer capacity exceeded", RedisErrorCode.CorruptData);
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
            ValidateNotCompleted();

            GetBuffer()[m_WritePosition] = val;
            IncrementWritePosition();
        }

        public void Write(byte[] data, int index, int length)
        {
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

                    var currPosition = m_WritePosition;
                    if (length > m_Capacity - currPosition)
                        throw new ArgumentException("Length can not exceed buffer capacitye", "length");

                    var buffer = GetBuffer();

                    System.Buffer.BlockCopy(data, index, buffer, currPosition, length);
                    IncrementWritePosition(length);
                }
            }
        }

        private byte[] GetBuffer()
        {
            var buffer = m_Buffer;
            if (buffer == null)
            {
                buffer = m_Buffer = new byte[m_Capacity];
                Reset();
            }
            return buffer;
        }

        private void IncrementReadPosition(int inc = 1)
        {
            m_ReadPosition = Math.Min(m_Capacity, Math.Max(Beginning, m_ReadPosition + inc));
        }

        private void IncrementWritePosition(int inc = 1)
        {
            m_WritePosition = Math.Min(m_Capacity, Math.Max(Beginning, m_WritePosition + inc));
        }

        public void Reset()
        {
            m_ReadPosition = Beginning;
            m_WritePosition = Beginning;
        }

        public int ReleaseBuffer(out byte[] data)
        {
            data = m_Buffer;
            m_Buffer = new byte[m_Capacity];

            var pos = m_WritePosition;

            Reset();

            return pos;
        }

        #endregion Methods
    }
}
