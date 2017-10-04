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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisResponseReader : RedisDisposable, IRedisReceiver
    {
        #region CRLFState

        protected enum CRLFState : int
        {
            None = 0,
            CR = 1,
            CRLF = 2
        }

        #endregion CRLFState

        #region Constants

        public const int MaxBufferSize = 32 * 1024;
        public const int DefaultBufferSize = 1024;

        protected const int Beginning = (int)RedisConstants.Zero;

        #endregion Constants

        #region Field Members

        private int m_ReceiveTimeout;
        private RedisSettings m_Settings;

        protected long m_ReadState;
        protected long m_ReceiveStarted;

        private int m_BufferSize;
        private byte[] m_Buffer;

        protected int m_WritePosition;
        protected int m_ReadPosition;

        #endregion Field Members

        #region .Ctors

        protected RedisResponseReader(RedisSettings settings, int bufferSize = -1)
        {
            m_Settings = settings ?? RedisSettings.Default;
            m_BufferSize = Math.Min(MaxBufferSize, Math.Max(DefaultBufferSize, Math.Max(0, bufferSize)));
            m_Buffer = new byte[m_BufferSize];

            CaclulateReceiveTimeout();
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);
            EndReading();

            Interlocked.Exchange(ref m_Settings, null);
        }

        #endregion Destructors

        #region Properties

        protected byte[] Buffer
        {
            get { return m_Buffer; }
        }

        protected int BufferSize
        {
            get { return m_BufferSize; }
        }

        public Exception Error { get; protected set; }

        public virtual bool ReceiveStarted
        {
            get { return Interlocked.Read(ref m_ReceiveStarted) != RedisConstants.Zero; }
        }

        public virtual bool Receiving
        {
            get { return (!Disposed && Interlocked.Read(ref m_ReadState) != RedisConstants.Zero); }
        }

        public int ReadPosition
        {
            get { return Math.Max(Beginning, Math.Min(BufferSize, m_ReadPosition)); }
        }

        public RedisSettings Settings
        {
            get { return m_Settings; }
        }

        public int WritePosition
        {
            get { return Math.Max(Beginning, Math.Min(BufferSize, m_WritePosition)); }
        }

        #endregion Properties

        #region Methods

        protected virtual bool BeginReading()
        {
            return Interlocked.CompareExchange(ref m_ReadState, RedisConstants.One, RedisConstants.Zero) == RedisConstants.Zero;
        }

        protected virtual bool EndReading()
        {
            return Interlocked.Exchange(ref m_ReadState, RedisConstants.Zero) == RedisConstants.One;
        }

        protected virtual RedisRawResponse ReadResponse(RedisSocket socket)
        {
            if (socket == null)
                throw new ArgumentNullException("socket");

            ValidateNotDisposed();

            if (socket.IsConnected())
            {
                Error = null;
                try
                {
                    var result = ProcessResponse(socket);
                    OnResponse(result);

                    return result;
                }
                catch (Exception e)
                {
                    Error = e;
                }
            }
            return RedisVoidResponse.Void;
        }

        protected virtual void OnResponse(IRedisRawResponse response)
        { }

        protected RedisRawResponse ProcessResponse(RedisSocket socket)
        {
            var b = ReadByte(socket);
            if (b < 0)
            {
                if (!Receiving)
                    return null;

                throw new RedisException("Unexpected byte for redis response type");
            }

            var item = new RedisRawResponse();

            item.SetTypeByte(b);
            if (item.Type == RedisRawObjectType.Undefined)
                throw new RedisException("Undefined redis response type");

            var data = ReadLine(socket);
            if (data == null && !Receiving)
                return null;

            switch (item.Type)
            {
                case RedisRawObjectType.Integer:
                case RedisRawObjectType.SimpleString:
                case RedisRawObjectType.Error:
                    item.SetData(data);
                    SetReady(item);
                    break;
                case RedisRawObjectType.BulkString:
                    {
                        var lenStr = Encoding.UTF8.GetString(data);
                        if (String.IsNullOrEmpty(lenStr))
                            throw new RedisException("Corrupted redis response, empty length for bulk string");

                        int msgLength;
                        if (!int.TryParse(lenStr, out msgLength))
                            throw new RedisException("Corrupted redis response, not an integer value for bulk string");

                        item.SetLength(Math.Max(-1, msgLength));
                        if (item.Length == -1)
                        {
                            item.SetData(null);
                        }
                        else
                        {
                            if (item.Length == 0)
                                item.SetData(new byte[0]);
                            else
                            {
                                data = ReadBytes(socket, item.Length);
                                if (data == null && !Receiving)
                                    return null;

                                item.SetData(data);
                            }

                            if (!EatCRLF(socket))
                                return null;
                        }
                        SetReady(item);
                    }
                    break;
                case RedisRawObjectType.Array:
                    {
                        var lenStr = Encoding.UTF8.GetString(data);
                        if (String.IsNullOrEmpty(lenStr))
                            throw new RedisException("Corrupted redis response, empty length for array");

                        int arrayLen;
                        if (!int.TryParse(lenStr, out arrayLen))
                            throw new RedisException("Corrupted redis response, not an integer value for array");

                        arrayLen = Math.Max(-1, arrayLen);
                        item.SetLength(arrayLen);

                        if (arrayLen > 0)
                        {
                            for (var i = 0; i < arrayLen; i++)
                            {
                                var child = ProcessResponse(socket);
                                if (child == null)
                                {
                                    if (!Receiving)
                                        return null;

                                    throw new RedisException("Unexpected response data, not valid data for array item");
                                }

                                item.Add(child);
                            }
                        }
                    }
                    break;
            }
            return item;
        }

        protected static void SetReady(RedisRawResponse child)
        {
            child.SetReady(true);

            var parent = child.Parent as RedisRawResponse;
            if (parent != null)
            {
                var count = parent.ChildCount;
                if (count == 0 || count == parent.Length)
                    SetReady(parent);
            }
        }

        protected bool EatCRLF(RedisSocket socket)
        {
            var data = ReadBytes(socket, RedisConstants.CRLFLength);
            if ((data == null || data.Length != RedisConstants.CRLFLength ||
               data[0] != '\r' || data[1] != '\n'))
            {
                if (!Receiving)
                    return false;
                throw new RedisException("Corrupted redis response, not a line end");
            }
            return true;
        }

        protected bool TryToReceive(RedisSocket socket, int length, out int receivedLength)
        {
            receivedLength = 0;
            if (m_WritePosition == Beginning || m_ReadPosition > m_WritePosition - 1)
            {
                receivedLength = BeginReceive(socket, length);
                return receivedLength > 0;
            }
            return true;
        }

        private void CaclulateReceiveTimeout()
        {
            var receiveTimeout = GetLoopedReceiveTimeout();
            if (receiveTimeout < 0)
                receiveTimeout = RedisConstants.DefaultReceiveTimeout;

            m_ReceiveTimeout = receiveTimeout;
        }

        protected virtual int GetLoopedReceiveTimeout()
        {
            var result = m_Settings != null ?
                m_Settings.ReceiveTimeout : RedisConstants.DefaultReceiveTimeout;

            if (result < 0)
                result = RedisConstants.DefaultReceiveTimeout;

            return result;
        }

        private int BeginReceive(RedisSocket socket, int length)
        {
            if ((length != 0) && socket.IsConnected() && (Interlocked.Read(ref m_ReadState) != RedisConstants.Zero) &&
                (Interlocked.CompareExchange(ref m_ReceiveStarted, RedisConstants.One, RedisConstants.Zero) == RedisConstants.Zero))
            {
                try
                {
                    var availableLength = BufferSize - m_WritePosition;
                    if (availableLength < 1)
                    {
                        Interlocked.Exchange(ref m_WritePosition, Beginning);
                        Interlocked.Exchange(ref m_ReadPosition, Beginning);
                        availableLength = BufferSize;
                    }

                    if (length > 0)
                        length = Math.Max(length, socket.Available);

                    var receiveSize = (length < 0) ? availableLength : Math.Min(length, availableLength);

                    var timeout = false;
                    var received = false;

                    double receiveTimeout = m_ReceiveTimeout;
                    var infiniteTimeout = (long)receiveTimeout == Timeout.Infinite;

                    var readStatus = SocketError.Success;
                    do
                    {
                        try
                        {
                            var now = DateTime.UtcNow;

                            var receivedLength = socket.Receive(m_Buffer, m_WritePosition, receiveSize, SocketFlags.None, out readStatus);
                            if (readStatus == SocketError.TimedOut ||
                                readStatus == SocketError.WouldBlock)
                            {
                                if (!infiniteTimeout)
                                {
                                    receiveTimeout -= (DateTime.UtcNow - now).TotalMilliseconds;
                                    if (receiveTimeout <= 0)
                                    {
                                        timeout = true;
                                        throw new SocketException((int)SocketError.TimedOut);
                                    }
                                }
                                continue;
                            }

                            received = true;
                            if (receivedLength > 0)
                                IncrementWritePosition(receivedLength);
                            else if (receivedLength == 0)
                                Interlocked.Exchange(ref m_ReadState, RedisConstants.Zero);

                            return receivedLength;
                        }
                        catch (SocketException e)
                        {
                            if (!infiniteTimeout ||
                                !(e.SocketErrorCode == SocketError.TimedOut ||
                                  e.SocketErrorCode == SocketError.WouldBlock))
                                throw;
                        }
                        catch (Exception e)
                        {
                            if (!(e is SocketException))
                                Interlocked.Exchange(ref m_ReceiveStarted, RedisConstants.Zero);
                        }
                    }
                    while (!(received || timeout) && (Interlocked.Read(ref m_ReadState) != RedisConstants.Zero));
                }
                finally
                {
                    Interlocked.Exchange(ref m_ReceiveStarted, RedisConstants.Zero);
                }
            }
            return int.MinValue;
        }

        protected int ReadByte(RedisSocket socket)
        {
            int receivedLength;
            if (TryToReceive(socket, 1, out receivedLength))
            {
                var b = m_Buffer[m_ReadPosition];
                IncrementReadPosition();

                return b;
            }
            return receivedLength;
        }

        protected void IncrementReadPosition(int inc = 1)
        {
            Interlocked.Exchange(ref m_ReadPosition, Math.Min(BufferSize, Math.Max(Beginning, m_ReadPosition + inc)));
        }

        protected void IncrementWritePosition(int inc = 1)
        {
            Interlocked.Exchange(ref m_WritePosition, Math.Min(BufferSize, Math.Max(Beginning, m_WritePosition + inc)));
        }

        protected byte[] ReadLine(RedisSocket socket)
        {
            int receivedLength;
            if (TryToReceive(socket, -1, out receivedLength))
            {
                byte[] line;
                var state = TryReadLineFromBuffer(CRLFState.None, out line);
                if (state == CRLFState.CRLF)
                    return line;

                var list = new List<byte[]>();

                var readLength = 0;
                if (line != null && line.Length > 0)
                {
                    readLength = line.Length;
                    list.Add(line);
                }

                while (TryToReceive(socket, -1, out receivedLength))
                {
                    state = TryReadLineFromBuffer(state, out line);
                    if (line != null && line.Length > 0)
                    {
                        readLength += line.Length;
                        list.Add(line);
                    }

                    if (state == CRLFState.CRLF)
                        break;
                }

                if (!Receiving)
                    return null;

                var listCount = list.Count;
                if (listCount == 1)
                    return list[0];

                if (readLength == 0)
                    return new byte[0];

                line = list[listCount - 1];
                if (line.Length > 0 && line[line.Length - 1] == '\n')
                    readLength--;

                var offset = 0;
                var result = new byte[readLength];

                for (var i = 0; i < listCount; i++)
                {
                    line = list[i];

                    if (i == listCount - 1)
                        System.Buffer.BlockCopy(line, 0, result, offset, readLength);
                    else
                    {
                        System.Buffer.BlockCopy(line, 0, result, offset, line.Length);

                        offset += line.Length;
                        readLength -= line.Length;
                    }
                }
                return result;
            }
            return null;
        }

        protected CRLFState TryReadLineFromBuffer(CRLFState currState, out byte[] line)
        {
            line = null;

            byte b;
            var startPos = m_ReadPosition;
            var stopPos = m_WritePosition;

            for (; m_ReadPosition < stopPos; IncrementReadPosition())
            {
                b = m_Buffer[m_ReadPosition];
                switch (b)
                {
                    case (byte)'\r':
                        currState = CRLFState.CR;
                        break;
                    case (byte)'\n':
                        if (currState == CRLFState.CR)
                        {
                            IncrementReadPosition();
                            currState = CRLFState.CRLF;
                            line = CopyBuffer(startPos, m_ReadPosition - startPos - 2);
                            return currState;
                        }
                        break;
                    default:
                        currState = CRLFState.None;
                        break;
                }
            }

            line = CopyBuffer(startPos, stopPos - startPos);
            return currState;
        }

        protected byte[] CopyBuffer(int offset, int length)
        {
            if (length < 0)
                return null;

            var result = new byte[length];
            if (length > 0)
                System.Buffer.BlockCopy(m_Buffer, offset, result, 0, length);

            return result;
        }

        protected byte[] ReadBytes(RedisSocket socket, int length)
        {
            if (length < 0)
                return null;

            if (length == 0)
                return new byte[0];

            int receivedLength;
            if (TryToReceive(socket, length, out receivedLength))
            {
                byte[] data;
                var received = TryReadBytesFromBuffer(length, out data);
                if (received == length)
                    return data;

                if (received > 0)
                    length -= received;

                var readLength = 0;
                var list = new List<byte[]>();

                if (received > 0)
                {
                    readLength += received;
                    list.Add(data);
                }

                while (length > 0 && TryToReceive(socket, length, out receivedLength))
                {
                    received = TryReadBytesFromBuffer(length, out data);
                    if (received > 0)
                    {
                        readLength += received;
                        list.Add(data);

                        length -= received;
                    }
                }

                var listCount = list.Count;
                if (listCount == 1)
                    return list[0];

                if (readLength == 0)
                    return new byte[0];

                var offset = 0;
                var result = new byte[readLength];

                for (var i = 0; i < listCount; i++)
                {
                    data = list[i];

                    System.Buffer.BlockCopy(data, 0, result, offset, data.Length);
                    offset += data.Length;
                }
                return result;
            }
            return null;
        }

        protected int TryReadBytesFromBuffer(int length, out byte[] data)
        {
            data = null;

            var validDataSize = m_WritePosition - m_ReadPosition;
            if (validDataSize < 1)
                return 0;

            length = Math.Min(length, validDataSize);
            if (length == 0)
                data = new byte[0];
            else if (length > 0)
            {
                data = new byte[length];

                System.Buffer.BlockCopy(m_Buffer, m_ReadPosition, data, 0, length);
                IncrementReadPosition(length);
            }
            return length;
        }

        #endregion Methods
    }
}
