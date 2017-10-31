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
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisSocketWriter : RedisDisposable, IRedisWriter
    {
        #region Field Members

        private RedisSocket m_Socket;
        private bool m_OwnsSocket;
        private readonly bool m_UseAsyncIfNeeded;

        #endregion Field Members

        #region .Ctors

        public RedisSocketWriter(RedisSocket socket, bool useAsyncIfNeeded = true, bool ownsStream = false)
        {
            if (socket == null)
                throw new ArgumentNullException("socket");

            m_Socket = socket;
            m_OwnsSocket = ownsStream;
            m_UseAsyncIfNeeded = useAsyncIfNeeded;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            var socket = Interlocked.Exchange(ref m_Socket, null);
            if (m_OwnsSocket && socket != null)
                socket.DisposeSocket();
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
            return m_Socket.Send(new byte[] { val }, 1, SocketFlags.None);
        }

        public int Write(string val)
        {
            if (!String.IsNullOrEmpty(val))
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
                        return m_Socket.SendAsync(data, index, length).Result;

                    m_Socket.GetRealStream().Write(data, index, length);
                    return length;
                }
            }
            return 0;
        }

        #endregion Methods
    }
}
