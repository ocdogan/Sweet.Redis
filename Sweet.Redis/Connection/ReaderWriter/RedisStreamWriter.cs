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
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisStreamWriter : RedisDisposable, IRedisWriter
    {
        #region Field Members

        private Stream m_Stream;
        private bool m_OwnsStream;
        private readonly bool m_UseAsyncIfNeeded;

        #endregion Field Members

        #region .Ctors

        public RedisStreamWriter(Stream stream, bool useAsyncIfNeeded = true, bool ownsStream = false)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");

            m_Stream = stream;
            m_OwnsStream = ownsStream;
            m_UseAsyncIfNeeded = useAsyncIfNeeded;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            var stream = Interlocked.Exchange(ref m_Stream, null);
            if (stream != null)
            {
                var dispose = m_OwnsStream;
                try
                {
                    stream.Flush();
                }
                catch (Exception e) 
                { 
                    dispose = (e is IOException) || 
                        (e is SocketException); 
                }
                finally
                {
                    if (dispose) stream.Dispose();
                }
            }
        }

        #endregion Destructors

        #region Properties

        public bool UseAsyncIfNeeded
        {
            get { return m_UseAsyncIfNeeded; }
        }

        #endregion Properties

        #region Methods

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
            m_Stream.Write(new byte[] { val }, 0, 1);
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

                    if (m_UseAsyncIfNeeded && (dataLength > 512))
                        m_Stream.WriteAsync(data, index, length).Wait();
                    else
                        m_Stream.Write(data, index, length);
                    return dataLength;
                }
            }
            return 0;
        }

        #endregion Methods
    }
}
