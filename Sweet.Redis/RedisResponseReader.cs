using System;
using System.Text;
using System.Threading;

namespace Sweet.Redis
{
    internal class RedisResponseReader
    {
        #region Methods

        public IRedisResponse Execute(RedisConnection connection)
        {
            using (var buffer = new RedisByteBuffer())
                return ReadThrough(new RedisResponse(), connection, buffer);
        }

        private static void Receive(RedisConnection connection, RedisByteBuffer buffer)
        {
            if (connection == null || connection.Disposed)
                throw new RedisException("Can not establish connection to complete redis response read");

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

        private static IRedisResponse ReadThrough(RedisResponse item, RedisConnection connection, RedisByteBuffer buffer)
        {
            if (connection == null || connection.Disposed)
                throw new RedisException("Can not establish connection to complete redis response read");

            var type = item.Type;
            var receiveMore = true;

            while (receiveMore || !item.Ready)
            {
                if (!receiveMore)
                    receiveMore = NeedToReceiveMore(item, buffer, type);

                if (receiveMore)
                {
                    receiveMore = false;
                    Receive(connection, buffer);
                }

                if (item.Length < -1)
                {
                    type = ReadObjectType(item, buffer);
                    if (ReadHeader(item, buffer, out receiveMore))
                        return item;

                    if (receiveMore)
                        continue;
                }

                if (ReadBody(item, connection, buffer, out receiveMore))
                    return item;
            }
            return null;
        }

        private static bool NeedToReceiveMore(RedisResponse item, RedisByteBuffer buffer, RedisObjectType type)
        {
            var bufferLength = buffer.Length;
            return (bufferLength == 0) ||
                ((item.Length < -1 || type == RedisObjectType.Undefined) && bufferLength == 0) ||
                (type != RedisObjectType.Array && (item.Length > bufferLength - buffer.Position + RedisConstants.CRLFLength));
        }

        private static bool ReadHeader(RedisResponse item, RedisByteBuffer buffer, out bool receiveMore)
        {
            receiveMore = false;

            var header = buffer.ReadLine();

            receiveMore = (header == null);
            if (receiveMore)
                return false;

            switch (item.Type)
            {
                case RedisObjectType.SimpleString:
                case RedisObjectType.Error:
                case RedisObjectType.Integer:
                    {
                        item.Data = header;
                        SetReady(item);

                        return true;
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

                            return true;
                        }

                        if (msgLength == 0)
                        {
                            if (item.Type == RedisObjectType.BulkString)
                            {
                                item.Data = new byte[0];
                                SetReady(item);

                                receiveMore = !buffer.EatCRLF();
                                return !receiveMore;
                            }

                            SetReady(item);
                            return true;
                        }

                        receiveMore = ((item.Type == RedisObjectType.BulkString) &&
                                       (buffer.Length < buffer.Position + msgLength + RedisConstants.CRLFLength));
                    }
                    break;
            }

            return false;
        }

        private static bool ReadBody(RedisResponse item, RedisConnection connection, RedisByteBuffer buffer, out bool receiveMore)
        {
            receiveMore = false;
            if (connection == null || connection.Disposed)
                throw new RedisException("Can not establish connection to complete redis response read");

            switch (item.Type)
            {
                case RedisObjectType.BulkString:
                    {
                        if (item.Length > 0)
                        {
                            receiveMore = (buffer.Length < buffer.Position + item.Length + RedisConstants.CRLFLength);
                            if (receiveMore)
                                return false;

                            var data = buffer.Read(item.Length);

                            receiveMore = (data == null);
                            if (receiveMore)
                                return false;

                            item.Data = data;
                            SetReady(item);

                            receiveMore = !buffer.EatCRLF();

                            return !receiveMore;
                        }

                        if (item.Length == 0)
                        {
                            receiveMore = !buffer.EatCRLF();
                            return !receiveMore;
                        }

                        if (item.Length == -1)
                            return true;
                    }
                    break;
                case RedisObjectType.Array:
                    {
                        for (var i = 0; i < item.Length; i++)
                        {
                            var child = ReadThrough(new RedisResponse(), connection, buffer);
                            if (child == null)
                                throw new RedisException("Unexpected response data, not valid data for array item");

                            item.Add(child);
                        }
                        return true;
                    }
            }
            return false;
        }

        private static RedisObjectType ReadObjectType(RedisResponse item, RedisByteBuffer buffer)
        {
            var type = item.Type;
            if (type == RedisObjectType.Undefined)
            {
                var b = buffer.ReadByte();
                if (b < 0)
                    throw new RedisException("Unexpected byte for redis response type");

                type = ((byte)b).ResponseType();
                if (type == RedisObjectType.Undefined)
                    throw new RedisException("Undefined redis response type");

                item.Type = type;
            }
            return type;
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

        #endregion Methods
    }
}
