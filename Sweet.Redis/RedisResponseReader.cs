using System;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisResponseReader : RedisDisposable, IRedisResponseReader
    {
        #region Field Members

        private long m_Executing;
        private RedisConnection m_Connection;

        #endregion Field Members

        #region .Ctors

        public RedisResponseReader(RedisConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            m_Connection = connection;
        }

        #endregion .Ctors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            Interlocked.Exchange(ref m_Connection, null);
        }

        #endregion Destructors

        #region Properties

        public bool Executing
        {
            get { return Interlocked.Read(ref m_Executing) == 0L; }
        }

        #endregion Properties

        #region Methods

        public IRedisResponse Execute()
        {
            ValidateNotDisposed();

            if (Interlocked.CompareExchange(ref m_Executing, 1L, 0L) == 0L)
            {
                try
                {
                    using (var buffer = new RedisByteBuffer())
                    {
                        return ReadThrough(new RedisResponse(), buffer);
                    }
                }
                finally
                {
                    Interlocked.Exchange(ref m_Executing, 0L);
                }
            }
            return null;
        }

        private void Receive(RedisByteBuffer buffer, RedisConnection connection = null)
        {
            connection = connection ?? m_Connection;
            if (connection == null || connection.Disposed)
                throw new RedisException("Can not complete redis response parse, can not establish connection");

            RedisReceivedData receivedData;
            do
            {
                receivedData = connection.Receive();
                if (receivedData.IsEmpty)
                    continue;

                buffer.Put(receivedData.Data);
            }
            while (receivedData.Available > receivedData.Length);
        }

        private IRedisResponse ReadThrough(RedisResponse item, RedisByteBuffer buffer)
        {
            var connection = m_Connection;
            if (connection == null || connection.Disposed)
                throw new RedisException("Can not complete redis response parse, can not establish connection");

            var type = item.Type;
            var receiveMore = true;

            while (receiveMore || !item.Ready)
            {
                if (!receiveMore)
                {
                    var bufferLength = buffer.Length;
                    receiveMore = (bufferLength == 0) ||
                        ((item.Length < -1 || type == RedisObjectType.Undefined) && bufferLength == 0) ||
                        (type != RedisObjectType.Array && (item.Length > buffer.Length - buffer.Position + RedisConstants.CRLFLength));
                }

                if (receiveMore)
                {
                    receiveMore = false;
                    Receive(buffer, connection);
                }

                if (item.Length < -1)
                {
                    if (item.Type == RedisObjectType.Undefined)
                    {
                        var b = buffer.ReadByte();
                        if (b < 0)
                            throw new RedisException("Unexpected byte for redis response type");

                        type = ((byte)b).ResponseType();
                        if (type == RedisObjectType.Undefined)
                            throw new RedisException("Undefined redis response type");

                        item.Type = type;
                    }

                    var header = buffer.ReadLine();

                    receiveMore = (header == null);
                    if (receiveMore)
                        continue;

                    switch (item.Type)
                    {
                        case RedisObjectType.SimpleString:
                        case RedisObjectType.Error:
                        case RedisObjectType.Integer:
                            {
                                item.Data = header;
                                SetReady(item);

                                return item;
                            }
                        default:
                            {
                                var lenStr = Encoding.UTF8.GetString(header);
                                if (String.IsNullOrEmpty(lenStr))
                                    throw new RedisException("Corrupted redis response, empty length string");

                                int msgLength;
                                if (!int.TryParse(lenStr, out msgLength))
                                    throw new RedisException("Corrupted redis response, not an integer value");

                                msgLength = Math.Max(-1, msgLength);
                                item.Length = msgLength;

                                if (msgLength == -1)
                                {
                                    item.Data = null;
                                    SetReady(item);

                                    return item;
                                }

                                if (msgLength == 0)
                                {
                                    if (item.Type == RedisObjectType.BulkString)
                                    {
                                        item.Data = new byte[0];
                                        SetReady(item);

                                        receiveMore = !buffer.EatCRLF();
                                        if (receiveMore)
                                            continue;

                                        return item;
                                    }

                                    SetReady(item);
                                    return item;
                                }

                                receiveMore = ((item.Type == RedisObjectType.BulkString) &&
                                               (buffer.Length < buffer.Position + msgLength + RedisConstants.CRLFLength));
                            }
                            break;
                    }

                    if (receiveMore)
                        continue;
                }

                switch (item.Type)
                {
                    case RedisObjectType.BulkString:
                        {
                            if (item.Length > 0)
                            {
                                receiveMore = (buffer.Length < buffer.Position + item.Length + RedisConstants.CRLFLength);
                                if (receiveMore)
                                    continue;

                                var data = buffer.Read(item.Length);

                                receiveMore = (data == null);
                                if (receiveMore)
                                    continue;

                                item.Data = data;
                                SetReady(item);

                                receiveMore = !buffer.EatCRLF();
                                if (receiveMore)
                                    continue;

                                return item;
                            }

                            if (item.Length == 0)
                            {
                                receiveMore = !buffer.EatCRLF();
                                if (receiveMore)
                                    continue;

                                return item;
                            }

                            if (item.Length == -1)
                                return item;
                        }
                        break;
                    case RedisObjectType.Array:
                        {
                            for (var i = 0; i < item.Length; i++)
                            {
                                var child = ReadThrough(new RedisResponse(), buffer);
                                if (child == null)
                                    throw new RedisException("Unexpected response data, not valid data for array item");

                                item.Add(child);
                            }

                            return item;
                        }
                }
            }
            return null;
        }

        private static void SetReady(RedisResponse child)
        {
            child.Ready = true;

            var parent = child.Parent as RedisResponse;
            if (parent != null)
            {
                var count = parent.Count;
                if (count == 0 || count == parent.Length)
                    SetReady(parent);
            }
        }

        #endregion Methods

    }
}
