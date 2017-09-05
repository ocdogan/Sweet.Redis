using System;
using System.Collections.Generic;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisByteBuffer : RedisDisposable
    {
        #region Constants

        private const int PutChunkLimit = 1024;
        private const int MaxChunkLength = 80 * 1024;

        #endregion Constants

        #region Field Members

        private long m_Length;
        private long m_Position;
        private List<byte[]> m_Chunks;

        #endregion Field Members

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Clear();
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

        public int ReadByte()
        {
            ValidateNotDisposed();

            var index = Interlocked.Read(ref m_Position);
            var currLength = Interlocked.Read(ref m_Length);

            if (index >= currLength)
                return -1;

            index = Math.Min(Interlocked.Add(ref m_Position, 1) - 1, currLength);
            if (index == currLength)
                Interlocked.Exchange(ref m_Length, currLength);

            if (index < currLength)
            {
                var chunks = m_Chunks;
                if (chunks != null)
                {
                    var chunkCount = chunks.Count;

                    for (var i = 0; i < chunkCount; i++)
                    {
                        var chunk = chunks[i];

                        var chunkLength = chunk.Length;
                        if (chunkLength == 0)
                            continue;

                        if (chunkLength <= index)
                        {
                            index -= chunkLength;
                            continue;
                        }

                        return chunk[index];
                    }
                }
            }
            return -1;
        }

        public void RemoveFromStart(long length = -1)
        {
            ValidateNotDisposed();

            length = Math.Max(length, -1);
            if (length == 0)
                return;

            if (length == -1)
                Clear();
            else
            {
                var currLength = Interlocked.Read(ref m_Length);
                if (currLength > 0)
                {
                    length = Math.Min(currLength, length);

                    if (length == currLength)
                        Clear();
                    else
                    {
                        var chunks = m_Chunks;
                        if (chunks != null)
                        {
                            while (length > 0 && chunks.Count > 0)
                            {
                                var chunk = chunks[0];
                                var chunkLength = chunk.Length;

                                if (chunkLength <= length)
                                {
                                    chunks.RemoveAt(0);

                                    Interlocked.Add(ref m_Length, -chunkLength);
                                    Interlocked.Exchange(ref m_Position, Math.Max(Interlocked.Read(ref m_Position) - chunkLength, 0L));
                                    length -= chunkLength;
                                }
                                else
                                {
                                    var newLength = chunkLength - length;

                                    var newChunk = new byte[newLength];
                                    Buffer.BlockCopy(chunk, (int)length, newChunk, 0, (int)newLength);

                                    chunks[0] = newChunk;

                                    Interlocked.Add(ref m_Length, -length);
                                    Interlocked.Exchange(ref m_Position, Math.Max(Interlocked.Read(ref m_Position) - length, 0L));
                                    length = 0;
                                }
                            }
                        }
                    }
                }
            }
        }

        public byte[] Read(int length = -1)
        {
            ValidateNotDisposed();

            length = Math.Max(-1, length);
            if (length == 0)
                return null;

            var chunks = m_Chunks;
            if (chunks == null)
                return null;

            var chunkCount = chunks.Count;
            if (chunkCount == 0)
                return null;

            var currPosition = Interlocked.Read(ref m_Position);
            var currLength = Interlocked.Read(ref m_Length) - currPosition;

            if (currLength <= 0)
                return null;

            if (length > currLength)
                return null;

            if (length == -1)
                length = (int)Math.Min(int.MaxValue, currLength);

            var index = 0;
            var result = new byte[length];

            for (var i = 0; length > 0 && i < chunkCount; i++)
            {
                var chunk = chunks[i];
                var chunkLength = chunk.Length;

                if (currPosition >= chunkLength)
                {
                    currPosition -= chunkLength;
                    continue;
                }

                var copyLength = Math.Min(length, (int)(chunkLength - currPosition));

                Buffer.BlockCopy(chunk, (int)currPosition, result, index, copyLength);

                currPosition = 0;
                Interlocked.Add(ref m_Position, copyLength);

                index += copyLength;
                length -= copyLength;
            }

            return result;
        }

        public bool ReadInto(byte[] data, int index = 0, int length = -1)
        {
            ValidateNotDisposed();

            if (data == null)
                return false;

            var dataLength = data.Length;
            if (dataLength == 0)
                return false;

            index = Math.Max(0, index);

            if (length == -1)
                length = dataLength - index;

            if (length > dataLength - index)
                return false;

            var chunks = m_Chunks;
            if (chunks == null)
                return false;

            var currPosition = Interlocked.Read(ref m_Position);
            var currLength = Interlocked.Read(ref m_Length) - currPosition;

            if (currLength <= 0)
                return false;

            if (length > currLength)
                return false;

            var chunkCount = chunks.Count;

            for (var i = 0; length > 0 && i < chunkCount; i++)
            {
                var chunk = chunks[i];
                var chunkLength = chunk.Length;

                if (currPosition >= chunkLength)
                {
                    currPosition -= chunkLength;
                    continue;
                }

                var copyLength = Math.Min(length, (int)(chunkLength - currPosition));

                Buffer.BlockCopy(chunk, (int)currPosition, data, index, copyLength);

                currPosition = 0;
                Interlocked.Add(ref m_Position, copyLength);

                index += copyLength;
                length -= copyLength;
            }

            return true;
        }

        public void Put(byte[] data)
        {
            ValidateNotDisposed();

            if (data == null)
                return;

            var dataLength = data.Length;
            if (dataLength == 0)
                return;

            var currLength = Interlocked.Read(ref m_Length);
            if (currLength + dataLength > int.MaxValue)
                throw new RedisException("Buffer size exceeded maximum possible size");

            var chunks = m_Chunks;
            if (chunks == null)
            {
                chunks = new List<byte[]>();
                Interlocked.Exchange(ref m_Chunks, chunks);
            }

            if (dataLength < PutChunkLimit && chunks.Count > 0)
            {
                Write(data, 0, dataLength);
                return;
            }

            chunks.Add(data);
            Interlocked.Add(ref m_Length, dataLength);
        }

        public void Write(byte[] data, int index = 0, int length = -1)
        {
            ValidateNotDisposed();

            if (data == null)
                return;

            var dataLength = data.Length;
            if (dataLength == 0)
                return;

            index = Math.Max(0, index);
            if (index > dataLength - 1)
                return;

            if (length == -1)
                length = dataLength - index;

            length = Math.Min(length, dataLength - index);

            var chunks = m_Chunks;
            if (chunks == null)
            {
                chunks = new List<byte[]>();
                Interlocked.Exchange(ref m_Chunks, chunks);
            }

            while (length > 0)
            {
                var currLength = Interlocked.Read(ref m_Length);

                var copyLength = Math.Min(length, MaxChunkLength);
                var chunk = (chunks.Count > 0) ? chunks[chunks.Count - 1] : null;

                if (chunk == null)
                {
                    if (currLength + copyLength > int.MaxValue)
                    {
                        copyLength = int.MaxValue - (int)currLength;
                        if (copyLength <= 0)
                            throw new RedisException("Buffer size exceeded maximum possible size");
                    }

                    chunk = new byte[copyLength];

                    Buffer.BlockCopy(data, index, chunk, 0, copyLength);
                    chunks.Add(chunk);
                }
                else
                {
                    var chunkLength = chunk.Length;

                    copyLength = Math.Min(copyLength, MaxChunkLength - chunkLength);
                    if (copyLength > 0)
                    {
                        if (currLength + copyLength > int.MaxValue)
                        {
                            copyLength = int.MaxValue - (int)currLength;
                            if (copyLength <= 0)
                                throw new RedisException("Buffer size exceeded maximum possible size");
                        }

                        var oldChunk = Interlocked.Exchange(ref chunk, new byte[copyLength + chunkLength]);
                        if (oldChunk != null && chunkLength > 0)
                            Buffer.BlockCopy(oldChunk, 0, chunk, 0, chunkLength);

                        Buffer.BlockCopy(data, index, chunk, chunkLength, copyLength);
                        chunks[chunks.Count - 1] = chunk;
                    }
                    else
                    {
                        chunkLength = 0;
                        copyLength = Math.Min(length, MaxChunkLength);

                        if (currLength + copyLength > int.MaxValue)
                        {
                            copyLength = int.MaxValue - (int)currLength;
                            if (copyLength <= 0)
                                throw new RedisException("Buffer size exceeded maximum possible size");
                        }

                        chunk = new byte[copyLength];
                        Buffer.BlockCopy(data, index, chunk, chunkLength, copyLength);

                        chunks.Add(chunk);
                    }
                }

                length -= copyLength;
                index += copyLength;

                Interlocked.Add(ref m_Length, copyLength);
            }
        }

        public bool EatCRLF()
        {
            ValidateNotDisposed();

            var chunks = m_Chunks;
            if (chunks == null)
                return false;

            var chunkCount = chunks.Count;
            if (chunkCount == 0)
                return false;

            var currLength = Interlocked.Read(ref m_Length);
            if (currLength <= 0)
                return false;

            var currPosition = Interlocked.Read(ref m_Position);
            if (currPosition >= currLength)
                return false;

            for (var i = 0; i < chunkCount; i++)
            {
                var chunk = chunks[i];
                var chunkLength = chunk.Length;

                if (currPosition >= chunkLength)
                {
                    currPosition -= chunkLength;
                    continue;
                }

                if (chunk[currPosition] != '\r')
                    return false;

                currPosition++;
                if (currPosition < chunkLength)
                {
                    if (chunk[currPosition] != '\n')
                        return false;

                    Interlocked.Add(ref m_Position, 2);
                    return true;
                }

                while (++i < chunkCount)
                {
                    chunk = chunks[i];
                    if (chunk.Length > 0)
                    {
                        if (chunk[0] != '\n')
                            return false;

                        Interlocked.Add(ref m_Position, 2);
                        return true;
                    }
                }
            }
            return false;
        }

        public int ReadFromPos(int index = 0)
        {
            index = Math.Max(0, index);
            if (index < Length)
            {
                var chunks = m_Chunks;
                if (chunks != null)
                {
                    var chunkCount = chunks.Count;

                    for (var i = 0; i < chunkCount; i++)
                    {
                        var chunk = chunks[i];
                        var chunkLength = chunk.Length;

                        if (chunkLength <= index)
                        {
                            index -= chunkLength;
                            continue;
                        }

                        return chunk[index];
                    }
                }
            }
            return -1;
        }

        public byte[] ReadLine()
        {
            ValidateNotDisposed();

            var chunks = m_Chunks;
            if (chunks == null)
                return null;

            var chunkCount = chunks.Count;
            if (chunkCount == 0)
                return null;

            var currLength = Interlocked.Read(ref m_Length);
            if (currLength <= 0)
                return null;

            var currPosition = Interlocked.Read(ref m_Position);

            var srcIndex = -1;
            var srcPosition = -1;

            var crPos = -1;
            var crIndex = -1;
            var lfPos = -1;
            var lfIndex = -1;

            var resultLength = 0;

            for (var i = 0; i < chunkCount; i++)
            {
                var chunk = chunks[i];
                var chunkLength = chunk.Length;

                if (currPosition >= chunkLength)
                {
                    currPosition -= chunkLength;
                    continue;
                }

                var startFrom = 0;
                if (srcIndex == -1)
                {
                    srcIndex = i;
                    srcPosition = (int)currPosition;
                    startFrom = srcPosition;
                }

                for (var j = startFrom; j < chunkLength; j++)
                {
                    switch (chunk[j])
                    {
                        case (byte)'\r':
                            crPos = j;
                            crIndex = i;
                            break;
                        case (byte)'\n':
                            if (crPos > -1)
                            {
                                lfPos = j;
                                lfIndex = i;
                            }
                            break;
                        default:
                            if (crPos > -1)
                            {
                                crPos = -1;
                                crIndex = -1;
                            }
                            break;
                    }

                    if (lfPos > -1)
                        break;
                }

                if (lfPos > -1)
                {
                    resultLength += crPos - startFrom;
                    break;
                }

                resultLength += chunkLength - startFrom;
            }

            if (lfIndex > -1)
            {
                var destPosition = 0;
                var result = new byte[resultLength];

                for (var i = srcIndex; i < chunkCount; i++)
                {
                    var chunk = chunks[i];

                    var chunkLength = chunk.Length;
                    if (chunkLength == 0)
                        continue;

                    var copyLength = (i == crIndex) ? crPos : chunkLength;
                    if (i == srcIndex)
                        copyLength -= srcPosition;

                    if (copyLength > 0)
                    {
                        Buffer.BlockCopy(chunk, srcPosition, result, destPosition, copyLength);
                        destPosition += copyLength;
                    }

                    if (i == crIndex)
                        break;
                }

                Interlocked.Add(ref m_Position, resultLength + 2);
                return result;
            }

            return null;
        }

        public bool HasLineEnd()
        {
            var hasLF = false;

            var chunks = m_Chunks;
            if (chunks != null)
            {
                for (var i = chunks.Count - 1; i > -1; i--)
                {
                    var chunk = chunks[i];

                    var chunkLength = chunk.Length;
                    if (chunkLength > 0)
                    {
                        if (hasLF)
                            return chunk[chunkLength - 1] == '\r';

                        hasLF = chunk[chunkLength - 1] == '\n';
                        if (!hasLF)
                            return false;

                        if (chunkLength > 1)
                            return chunk[chunkLength - 2] == '\r';
                    }
                }

            }
            return false;
        }

        public void Clear()
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

            Interlocked.Exchange(ref m_Position, 0);
            var currLength = Interlocked.Exchange(ref m_Length, 0);

            var chunks = Interlocked.Exchange(ref m_Chunks, null);
            if (chunks == null)
                return null;

            var chunkCount = chunks.Count;
            if (chunkCount == 0)
                return null;

            var result = new byte[currLength];

            var index = 0;
            for (var i = 0; i < chunkCount; i++)
            {
                var chunk = chunks[i];
                var chunkLength = chunk.Length;

                if (chunkLength > 0)
                {
                    Buffer.BlockCopy(chunk, 0, result, index, chunkLength);
                    index += chunkLength;
                }
            }

            chunks.Clear();

            return result;
        }

        public byte[] ToArray()
        {
            var currLength = Interlocked.Read(ref m_Length);
            if (currLength == 0)
                return null;

            var chunks = m_Chunks;
            if (chunks == null)
                return null;

            var chunkCount = chunks.Count;
            if (chunkCount == 0)
                return null;

            var result = new byte[currLength];

            var index = 0;
            for (var i = 0; i < chunkCount; i++)
            {
                var chunk = chunks[i];
                var chunkLength = chunk.Length;

                if (chunkLength > 0)
                {
                    Buffer.BlockCopy(chunk, 0, result, index, chunkLength);
                    index += chunkLength;
                }
            }
            return result;
        }

        #endregion Methods
    }
}
