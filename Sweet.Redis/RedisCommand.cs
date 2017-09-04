using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

namespace Sweet.Redis
{
    internal class RedisCommand : RedisDisposable
    {
        #region Field Members

		private int m_RequestLength;
		private byte[] m_Command;
        private byte[][] m_Arguments;

        #endregion Field Members

        #region .Ctors

		public RedisCommand(byte[] command, params byte[][] args)
		{
			if (command == null)
				throw new ArgumentNullException("command");

			m_Command = command;
			m_Arguments = args;

			m_RequestLength = CalculateRequestLengh(m_Command, m_Arguments);
		}
		
        #endregion .Ctors

		#region Destructors

        protected override void OnDispose(bool disposing)
        {
            m_Arguments = null;
        }

        #endregion Destructors

        #region Properties

        public byte[] Command { get { return m_Command; } }

        private byte[][] Arguments { get { return m_Arguments; } }

		#endregion Properties

		#region Methods

		private static int CalculateRequestLengh(byte[] command, byte[][] args)
		{
			var argsLen = args != null ? args.Length : 0;

			var length = 1; // * sign
			length += ((argsLen + 1) / 10) + 1; // total length
			length += 2; // total length line end
			length += 1; // $ sign
			length += (command.Length / 10) + 1; // commang length line
			length += 2; // command length line end
			length += command.Length;
			length += 2; // command line end

			if (argsLen > 0)
			{
				foreach (var arg in args)
				{
					if (arg == null)
					{
						length += 5; // $-1\r\n length
					}
					else if (arg.Length == 0)
					{
						length += 6; // $0\r\n\r\n length
					}
					else
					{
						length += 1; // $ sign
						length += (arg.Length / 10) + 1; // arg length line
						length += 2; // arg length line end
						length += arg.Length;
						length += 2; // arg line end
					}
				}
			}
			return length;
		}

		private byte[] PrepareData()
		{
			var argsLen = m_Arguments != null ? m_Arguments.Length : 0;
			var bufferLen = Math.Min(m_RequestLength + 16, RedisConstants.DefaultBufferSize);

			using (var ms = new MemoryStream(m_RequestLength))
            {
                using (var bw = new BinaryWriter(new BufferedStream(ms, bufferLen), Encoding.UTF8))
                {
                    bw.Write('*');
                    bw.Write((argsLen + 1).ToBytes());
                    bw.Write(RedisConstants.LineEnd);
                    bw.Write('$');
                    bw.Write(m_Command.Length.ToBytes());
                    bw.Write(RedisConstants.LineEnd);
                    bw.Write(m_Command);
                    bw.Write(RedisConstants.LineEnd);

                    if (argsLen > 0)
                    {
                        byte[] arg;
                        for (var i = 0; i < argsLen; i++)
                        {
                            arg = m_Arguments[i];

                            if (arg == null)
                            {
								bw.Write("$-1".ToBytes());
								bw.Write(RedisConstants.LineEnd);
							}
							else if (arg.Length == 0)
							{
								bw.Write("$0".ToBytes());
								bw.Write(RedisConstants.LineEnd);
                                bw.Write(RedisConstants.LineEnd);
							}
							else
                            {
                                bw.Write('$');
                                bw.Write(arg.Length.ToBytes());
                                bw.Write(RedisConstants.LineEnd);
                                bw.Write(arg);
                                bw.Write(RedisConstants.LineEnd);
                            }
                        }
                    } 
                }
                return ms.ToArray();
            }
		}

		public bool ExpectSimpleString(RedisConnectionPool pool, string expectedResult, bool throwException = true)
		{
            var result = ExpectSimpleString(pool, throwException);
            if (!String.IsNullOrEmpty(result))
            {
                if (!String.IsNullOrEmpty(expectedResult))
                    return result.Equals(expectedResult, StringComparison.OrdinalIgnoreCase);

                if (result.StartsWith("-", StringComparison.Ordinal))
                    return false;
                
                return true;
            }
            return false;
		}

		public string ExpectSimpleString(RedisConnectionPool pool, bool throwException = true)
		{
			var bytes = ExpectSimpleStringBytes(pool, throwException);
			if (bytes == null)
				return null;
			return Encoding.UTF8.GetString(bytes);
		}

		public bool ExpectSimpleStringBytes(RedisConnectionPool pool, byte[] expectedResult, bool throwException = true)
		{
			var result = ExpectSimpleStringBytes(pool, throwException);
            if (result != null && result.Length > 0)
			{
                if (expectedResult != null && expectedResult.Length > 0)
                    return result.Equals(expectedResult, (b1, b2) => char.ToUpper((char)b1) == char.ToUpper((char)b2));

				if (result[0] == '-')
					return false;

				return true;
			}
			return false;
		}

        public byte[] ExpectSimpleStringBytes(RedisConnectionPool pool, bool throwException = true)
		{
            using (var response = Execute(pool, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }

                if (response.Type != RedisObjectType.SimpleString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned");
                    return null;
                }
                return response.ReleaseData();
            }
		}

        public string ExpectBulkString(RedisConnectionPool pool, bool throwException = true)
		{
			var bytes = ExpectBulkStringBytes(pool, throwException);
			if (bytes == null)
				return null;
			return Encoding.UTF8.GetString(bytes);
		}

		public byte[] ExpectBulkStringBytes(RedisConnectionPool pool, bool throwException = true)
		{
            using (var response = Execute(pool, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }

                if (response.Type != RedisObjectType.BulkString)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned");
                    return null;
                }
                return response.ReleaseData();
            }
		}

        public long ExpectInteger(RedisConnectionPool pool, bool throwException = true)
		{
            using (var response = Execute(pool, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return long.MinValue;
                }

                if (response.Type != RedisObjectType.Integer)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned");
                    return long.MinValue;
                }

                var data = response.Data;
                if (data == null || data.Length == 0)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return long.MinValue;
                }

                long result;
                if (long.TryParse(Encoding.UTF8.GetString(data), out result))
                    return result;

                if (throwException)
                    throw new RedisException("Not an integer result");

                return long.MinValue;
            }
		}

		public double ExpectDouble(RedisConnectionPool pool, bool throwException = true)
		{
            using (var response = Execute(pool, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return double.MinValue;
                }

                if (response.Type == RedisObjectType.Array ||
                    response.Type == RedisObjectType.Undefined)
                {
                    if (throwException)
                        throw new RedisException("Invalid data returned");
                    return double.MinValue;
                }

                var data = response.Data;
                if (data == null || data.Length == 0)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return double.MinValue;
                }

                double result;
                if (double.TryParse(Encoding.UTF8.GetString(data), out result))
                    return result;

                if (throwException)
                    throw new RedisException("Not a double result");

                return double.MinValue;
            }
		}        	

        public RedisObject ExpectArray(RedisConnectionPool pool, bool throwException = true)
		{
            using (var response = Execute(pool, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }
                return RedisObject.ToObject(response);
            }
		}

		public string[] ExpectMultiDataStrings(RedisConnectionPool pool, bool throwException = true)
		{
            using (var response = Execute(pool, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }

                var data = response.Data;
                switch (response.Type)
                {
                    case RedisObjectType.SimpleString:
                    case RedisObjectType.BulkString:
                    case RedisObjectType.Integer:
                        return data != null ? new string[] { Encoding.UTF8.GetString(data) } : null;
                    case RedisObjectType.Error:
                        {
                            if (!throwException)
                                return data != null ? new string[] { Encoding.UTF8.GetString(data) } : null;
                            throw new RedisException(data != null && data.Length > 0 ? Encoding.UTF8.GetString(data) : "No data returned");
                        }
                    case RedisObjectType.Undefined:
                        if (throwException)
                            throw new RedisException("Undefined respone data");
                        return null;
                    case RedisObjectType.Array:
                        {
                            var len = response.Length;
                            if (len < 0)
                                return null;
                            if (len == 0)
                                return new string[0] { };

                            var items = response.Items;
                            if (items != null)
                            {
                                var list = new List<string>();

                                len = items.Count;
                                for (var i = 0; i < len; i++)
                                {
                                    var item = items[i];
                                    if (item == null)
                                        list.Add(null);
                                    else
                                    {
                                        if (item.Type == RedisObjectType.Undefined)
                                        {
                                            if (throwException)
                                                throw new RedisException("Undefined respone data");
                                            return null;
                                        }

                                        if (item.Type == RedisObjectType.Array)
                                        {
                                            if (throwException)
                                                throw new RedisException("Multi-array is not allowed for multi-data respone");
                                            return null;
                                        }

                                        data = item.Data;
                                        list.Add(data != null ? Encoding.UTF8.GetString(data) : null);
                                    }
                                }
                                return list.ToArray();
                            }

                            return null;
                        }
                }
                return null;
            }
		}

		public byte[][] ExpectMultiDataBytes(RedisConnectionPool pool, bool throwException = true)
		{
            using (var response = Execute(pool, throwException))
            {
                if (response == null)
                {
                    if (throwException)
                        throw new RedisException("No data returned");
                    return null;
                }

                var data = response.Data;
                switch (response.Type)
                {
                    case RedisObjectType.SimpleString:
                    case RedisObjectType.BulkString:
                    case RedisObjectType.Integer:
                        return data != null ? new byte[1][] { data } : null;
                    case RedisObjectType.Error:
                        {
                            if (!throwException)
                                return data != null ? new byte[1][] { data } : null;
                            throw new RedisException(data != null && data.Length > 0 ? Encoding.UTF8.GetString(data) : "No data returned");
                        }
                    case RedisObjectType.Undefined:
                        if (throwException)
                            throw new RedisException("Undefined respone data");
                        return null;
                    case RedisObjectType.Array:
                        {
                            var len = response.Length;
                            if (len < 0)
                                return null;
                            if (len == 0)
                                return new byte[0][] { };

                            var items = response.Items;
                            if (items != null)
                            {
                                var list = new List<byte[]>();

                                len = items.Count;
                                for (var i = 0; i < len; i++)
                                {
                                    var item = items[i];
                                    if (item == null)
                                        list.Add(null);
                                    else
                                    {
                                        if (item.Type == RedisObjectType.Undefined)
                                        {
                                            if (throwException)
                                                throw new RedisException("Undefined respone data");
                                            return null;
                                        }

                                        if (item.Type == RedisObjectType.Array)
                                        {
                                            if (throwException)
                                                throw new RedisException("Multi-array is not allowed for multi-data respone");
                                            return null;
                                        }

                                        list.Add(item.Data);
                                    }
                                }
                                return list.ToArray();
                            }

                            return null;
                        }
                }
                return null;
            }
		}

        public IRedisResponse Execute(RedisConnectionPool pool, bool throwException = true)
        {
			var data = PrepareData();
			using (var conn = pool.Connect())
			{
                var response = conn.Send(data);
                if (response == null && throwException)
                    throw new RedisException("Corrupted redis response data");
                HandleError(response);

                return response;
			}
		}

        private static void HandleError(IRedisResponse response)
		{
			if (response != null)
			{
                if (response.Type == RedisObjectType.Error)
                {
                    var data = response.Data;
                    throw new RedisException(data != null && data.Length > 0 ? Encoding.UTF8.GetString(data) : "No data returned");
                }

                var items = response.Items;
                if (items != null)
                    for (var i = items.Count - 1; i > -1; i--)
                        HandleError(items[i]);
			}
		}

		#endregion Methods
	}
}
