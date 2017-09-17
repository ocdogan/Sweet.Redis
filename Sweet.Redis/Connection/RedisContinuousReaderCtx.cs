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
    internal class RedisContinuousReaderCtx : RedisDisposable
    {
        #region CRLFState

        private enum CRLFState : int
        {
            None = 0,
            CR = 1,
            CRLF = 2
        }

        #endregion CRLFState

        #region Constants

        public const int BufferSize = 1024;

        #endregion Constants

        #region Field Members

        private long m_ReadState;

        private int m_WritePosition;
        private int m_ReadPosition;
        private byte[] m_Buffer = new byte[BufferSize];

        private Action<RedisContinuousReader, RedisResponse> m_OnReceive;

        private long m_ProcessingReceivedQ;
        private ConcurrentQueue<RedisResponse> m_ReceivedResponseQ = new ConcurrentQueue<RedisResponse>();

        private readonly object m_ReceivedCallbackResultsLock = new object();
        private LinkedList<RedisReceiveCallbackResult> m_ReceivedCallbackResults = new LinkedList<RedisReceiveCallbackResult>();

        #endregion Field Members

        #region .Ctors

        public RedisContinuousReaderCtx(RedisContinuousReader reader, RedisConnection connection, RedisSocket socket,
                              Action<RedisContinuousReader, RedisResponse> onReceive)
        {
            Reader = reader;
            Connection = connection;
            Socket = socket;
            m_OnReceive = onReceive;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            base.OnDispose(disposing);

            EndRead();

            Interlocked.Exchange(ref m_OnReceive, null);

            Reader = null;
            Connection = null;
            Socket = null;

            RedisResponse temp;
            while (m_ReceivedResponseQ.TryDequeue(out temp)) { }
        }

        #endregion Destructors

        #region Properties

        public Exception Error { get; private set; }

        public RedisSocket Socket { get; private set; }

        public RedisConnection Connection { get; private set; }

        public RedisContinuousReader Reader { get; private set; }

        public bool Receiving
        {
            get
            {
                if (!Disposed &&
                    Interlocked.Read(ref m_ReadState) != RedisConstants.Zero)
                {
                    var reader = Reader;
                    return reader != null && !reader.Disposed && reader.Receiving;
                }
                return false;
            }
        }

        public int ReadPosition
        {
            get { return Math.Max(0, Math.Min(BufferSize, m_ReadPosition)); }
        }

        public int WritePosition
        {
            get { return Math.Max(0, Math.Min(BufferSize, m_WritePosition)); }
        }

        #endregion Properties

        #region Methods

        private void EndRead()
        {
            Interlocked.Exchange(ref m_ReadState, RedisConstants.Zero);

            lock (m_ReceivedCallbackResultsLock)
            {
                while (m_ReceivedCallbackResults.Count > 0)
                {
                    try
                    {
                        var receiveCallback = m_ReceivedCallbackResults.First.Value;
                        m_ReceivedCallbackResults.RemoveFirst();

                        if (receiveCallback != null)
                            receiveCallback.Dispose();
                    }
                    catch (Exception)
                    { }
                }
            }
        }

        public void Read()
        {
            ValidateNotDisposed();

            if (Socket == null || Connection == null)
                return;

            if (Interlocked.CompareExchange(ref m_ReadState, RedisConstants.One, RedisConstants.Zero) ==
                RedisConstants.Zero)
            {
                try
                {
                    do
                    {
                        EnqueueResponse(ReadResponse());
                    }
                    while (Receiving);
                }
                catch (Exception e)
                {
                    Error = e;
                }
                finally
                {
                    Interlocked.Exchange(ref m_ReadState, RedisConstants.Zero);
                }
            }
        }

        private void EnqueueResponse(RedisResponse response)
        {
            if (response != null && Receiving)
            {
                m_ReceivedResponseQ.Enqueue(response);

                if (Interlocked.CompareExchange(ref m_ProcessingReceivedQ, RedisConstants.One, RedisConstants.Zero) ==
                    RedisConstants.Zero)
                {
                    Action qProcess = () =>
                    {
                        try
                        {
                            RedisResponse qItem;
                            while (m_ReceivedResponseQ.TryDequeue(out qItem))
                            {
                                try
                                {
                                    var onReceive = m_OnReceive;
                                    if (onReceive != null)
                                        onReceive.InvokeAsync(Reader, qItem);
                                }
                                catch (Exception)
                                { }
                            }
                        }
                        finally
                        {
                            Interlocked.Exchange(ref m_ProcessingReceivedQ, RedisConstants.Zero);
                        }
                    };

                    qProcess.InvokeAsync();
                }
            }
        }

        private RedisResponse ReadResponse()
        {
            return ProcessResponse();
        }

        private RedisResponse ProcessResponse()
        {
            var item = new RedisResponse();

            var b = ReadByte(Socket);
            if (b < 0)
                throw new RedisException("Unexpected byte for redis response type");

            item.TypeByte = b;
            if (item.Type == RedisObjectType.Undefined)
                throw new RedisException("Undefined redis response type");

            var data = ReadLine(Socket);
            switch (item.Type)
            {
                case RedisObjectType.Integer:
                case RedisObjectType.SimpleString:
                case RedisObjectType.Error:
                    item.Data = data;
                    SetReady(item);
                    break;
                case RedisObjectType.BulkString:
                    {
                        var lenStr = Encoding.UTF8.GetString(data);
                        if (String.IsNullOrEmpty(lenStr))
                            throw new RedisException("Corrupted redis response, empty length for bulk string");

                        int msgLength;
                        if (!int.TryParse(lenStr, out msgLength))
                            throw new RedisException("Corrupted redis response, not an integer value for bulk string");

                        item.Length = Math.Max(-1, msgLength);
                        if (item.Length == -1)
                        {
                            item.Data = null;
                        }
                        else
                        {
                            if (item.Length == 0)
                                item.Data = new byte[0];
                            else
                                item.Data = ReadBytes(Socket, item.Length);

                            EatCRLF(Socket);
                        }
                        SetReady(item);
                    }
                    break;
                case RedisObjectType.Array:
                    {
                        var lenStr = Encoding.UTF8.GetString(data);
                        if (String.IsNullOrEmpty(lenStr))
                            throw new RedisException("Corrupted redis response, empty length for array");

                        int arrayLen;
                        if (!int.TryParse(lenStr, out arrayLen))
                            throw new RedisException("Corrupted redis response, not an integer value for array");

                        arrayLen = Math.Max(-1, arrayLen);
                        item.Length = arrayLen;

                        if (arrayLen > 0)
                        {
                            for (var i = 0; i < arrayLen; i++)
                            {
                                var child = ProcessResponse();
                                if (child == null)
                                    throw new RedisException("Unexpected response data, not valid data for array item");

                                item.Add(child);
                            }
                        }
                    }
                    break;
            }

            return item;
        }

        private static void SetReady(RedisResponse child)
        {
            child.Ready = true;

            var parent = child.Parent as RedisResponse;
            if (parent != null)
            {
                var count = parent.ChildCount;
                if (count == 0 || count == parent.Length)
                    SetReady(parent);
            }
        }

        private void EatCRLF(RedisSocket socket)
        {
            var data = ReadBytes(socket, RedisConstants.CRLFLength);
            if (data == null || data.Length != RedisConstants.CRLFLength ||
               data[0] != '\r' || data[1] != '\n')
                throw new RedisException("Corrupted redis response, not a line end");
        }

        private bool TryToReceive(RedisSocket socket)
        {
            if (m_WritePosition == 0 || m_ReadPosition > m_WritePosition - 1)
                return Receive(socket) > 0;
            return true;
        }

        private int Receive(RedisSocket socket)
        {
            var receiveSize = BufferSize - m_WritePosition;
            if (receiveSize < 1)
            {
                m_WritePosition = 0;
                m_ReadPosition = 0;
                receiveSize = BufferSize;
            }

            var received = int.MinValue;
            var asyncResult = socket.BeginReceive(m_Buffer, m_WritePosition, receiveSize, SocketFlags.None,
                (ar) =>
                {
                    RedisReceiveCallbackResult callbackResult = null;
                    try
                    {
                        received = CompleteAsyncResult(ar, out callbackResult);
                        if (received > 0)
                            m_WritePosition += received;
                        else if (received == 0 || received == -1)
                            Interlocked.Exchange(ref m_ReadState, RedisConstants.Zero);
                    }
                    finally
                    {
                        if (callbackResult != null)
                            callbackResult.Dispose();
                    }
                }, socket);

            if (!asyncResult.IsCompleted)
            {
                using (var cr = new RedisReceiveCallbackResult(socket, asyncResult))
                {
                    lock (m_ReceivedCallbackResultsLock)
                    {
                        if (!cr.IsCompleted)
                            m_ReceivedCallbackResults.AddLast(cr);
                    }
                    cr.WaitOne();
                    RemoveCallbackResult(cr);
                }
            }
            return received;
        }

        private void RemoveCallbackResult(RedisReceiveCallbackResult callbackResult)
        {
            if (callbackResult != null)
            {
                lock (m_ReceivedCallbackResultsLock)
                {
                    if (m_ReceivedCallbackResults.Count > 0)
                    {
                        var node = m_ReceivedCallbackResults.First;
                        while (node != null)
                        {
                            var cr = node.Value;
                            if (cr != null && cr == callbackResult)
                            {
                                m_ReceivedCallbackResults.Remove(node);
                                return;
                            }
                            node = node.Next;
                        }
                    }
                }
            }
        }

        private int CompleteAsyncResult(IAsyncResult asyncResult, out RedisReceiveCallbackResult callbackResult)
        {
            callbackResult = null;
            if (asyncResult != null)
            {
                lock (m_ReceivedCallbackResultsLock)
                {
                    if (m_ReceivedCallbackResults.Count > 0)
                    {
                        var node = m_ReceivedCallbackResults.First;
                        while (node != null)
                        {
                            var cr = node.Value;
                            if (cr != null && cr.AsyncResult == asyncResult)
                            {
                                callbackResult = cr;
                                m_ReceivedCallbackResults.Remove(node);
                                return callbackResult.EndReceive();
                            }
                            node = node.Next;
                        }
                    }
                }
            }
            return int.MinValue;
        }

        private int ReadByte(RedisSocket socket)
        {
            if (TryToReceive(socket))
            {
                var b = m_Buffer[m_ReadPosition];
                IncReadPosition();

                return b;
            }
            return -1;
        }

        private void IncReadPosition(int inc = 1)
        {
            m_ReadPosition = Math.Min(BufferSize, Math.Max(0, m_ReadPosition + inc));
        }

        private byte[] ReadLine(RedisSocket socket)
        {
            if (TryToReceive(socket))
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

                while (TryToReceive(socket))
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
                        Buffer.BlockCopy(line, 0, result, offset, readLength);
                    else
                    {
                        Buffer.BlockCopy(line, 0, result, offset, line.Length);

                        offset += line.Length;
                        readLength -= line.Length;
                    }
                }
                return result;
            }
            return null;
        }

        private CRLFState TryReadLineFromBuffer(CRLFState currState, out byte[] line)
        {
            line = null;

            var startPos = m_ReadPosition;
            var stopPos = m_WritePosition;

            for (; m_ReadPosition < stopPos; IncReadPosition())
            {
                switch (m_Buffer[m_ReadPosition])
                {
                    case (byte)'\r':
                        currState = CRLFState.CR;
                        break;
                    case (byte)'\n':
                        if (currState == CRLFState.CR)
                        {
                            IncReadPosition();
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

        private byte[] CopyBuffer(int offset, int length)
        {
            if (length < 0)
                return null;

            var result = new byte[length];
            if (length > 0)
                Buffer.BlockCopy(m_Buffer, offset, result, 0, length);

            return result;
        }

        private byte[] ReadBytes(RedisSocket socket, int length)
        {
            if (length < 0)
                return null;

            if (length == 0)
                return new byte[0];

            if (TryToReceive(socket))
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

                while (length > 0 && TryToReceive(socket))
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
                    Buffer.BlockCopy(data, 0, result, offset, data.Length);
                    offset += data.Length;
                }
                return result;
            }
            return null;
        }

        private int TryReadBytesFromBuffer(int length, out byte[] data)
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

                Buffer.BlockCopy(m_Buffer, m_ReadPosition, data, 0, length);
                IncReadPosition(length);
            }
            return length;
        }

        #endregion Methods
    }
}
