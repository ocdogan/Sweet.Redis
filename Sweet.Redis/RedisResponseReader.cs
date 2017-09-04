using System;
using System.Net.Sockets;
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

        private IRedisResponse ReadThrough(RedisResponse item, RedisByteBuffer buffer)
        {
            var readMore = false;
			var type = item.Type;
			
            while (!item.Ready)
            {
                var bufferLength = buffer.Length;
                readMore = readMore || (bufferLength == 0) ||
                    ((item.Length < -1 || type == RedisObjectType.Undefined) && bufferLength == 0) ||
                    (type != RedisObjectType.Array && item.Length > buffer.Length - buffer.Position);
                
                if (readMore)
                {                    
					var connection = m_Connection;
					if (connection == null || connection.Disposed)
						throw new RedisException("Can not complete redis response parse");

                    var receivedData = connection.Receive();
                    if (receivedData.IsEmpty)
                        continue;

                    readMore = false;
                    buffer.Put(receivedData.Data);
				}

                if (item.Length < -1)
                {
                    if (item.Type == RedisObjectType.Undefined)
                    {
                        var b = buffer.ReadByte();
                        if (b < 0)
                            throw new RedisException("Undefined redis response type");

                        type = ((byte)b).ResponseType();
                        if (type == RedisObjectType.Undefined)
                            throw new RedisException("Undefined redis response type");

                        item.Type = type;
                    }

                    var header = buffer.ReadLine();
                    if (header == null)
                    {
                        readMore = true;
                        continue;
                    }

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
                                    throw new RedisException("Corrupted redis response");

                                int msgLen;
                                if (!int.TryParse(lenStr, out msgLen))
                                    throw new RedisException("Corrupted redis response");

                                msgLen = Math.Max(-1, msgLen);
                                item.Length = msgLen;

                                if (msgLen < 1)
                                {
                                    item.Data = (msgLen == 0) ? new byte[0] : null;
                                    SetReady(item);

                                    buffer.EatCRLF();

                                    return item;
                                }
                            }
                            break;
                    }
                    continue;
                }

                switch (item.Type)
                {
                    case RedisObjectType.BulkString:
                        {
                            var data = buffer.Read(item.Length);
                            if (data == null)
                                readMore = true;
                            else
                            {
                                item.Data = data;
                                SetReady(item);
								
                                buffer.EatCRLF();
								
                                return item;
                            }
                        }
                        break;
                    case RedisObjectType.Array:
                        {
                            for (var i = 0; i < item.Length; i++)
                            {
                                var child = ReadThrough(new RedisResponse(), buffer);
                                if (child == null)
                                    throw new RedisException("Unexpected response data");

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
