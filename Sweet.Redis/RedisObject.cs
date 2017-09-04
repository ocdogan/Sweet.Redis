using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace Sweet.Redis
{
    public class RedisObject
    {
        #region Field Members

        private ReadOnlyCollection<RedisObject> m_List;

        #endregion Field Members

        #region .Ctors

        public RedisObject(RedisObjectType type, object data)
        {
            Data = data;
            Type = type;
        }

        #endregion .Ctors

        #region Properties

        public int Count
        {
            get
            {
                if (Type != RedisObjectType.Array)
                    return -1;

                var list = m_List;
                return (list != null) ? list.Count : 0;
            }
        }

        public object Data { get; private set; }

        public IList<RedisObject> Items
        {
            get { return m_List; }
        }

        public RedisObjectType Type { get; private set; }

		#endregion Properties

		#region Methods

        public static RedisObject ToObject(IRedisResponse response)
        {
            if (response == null)
                return null;

            var type = response.Type;
            if (type == RedisObjectType.Undefined)
                throw new RedisException("Undefined redis response");

			object data = null;
			var bytes = response.Data;
            if (bytes != null)
            {
                switch (type)
                {
                    case RedisObjectType.SimpleString:
                    case RedisObjectType.BulkString:
					case RedisObjectType.Error:
						data = Encoding.UTF8.GetString(bytes);
                        break;
                    case RedisObjectType.Integer:
                        if (bytes.Length == 0)
                            throw new RedisException("Invalid integer value");
                        
                        long l;
                        if (!long.TryParse(Encoding.UTF8.GetString(bytes), out l))
                            throw new RedisException("Invalid integer value");

                        data = l;
                        break;
                }
            }

            var result = new RedisObject(type, data);
            if (type == RedisObjectType.Array && response.Length > -1)
            {
                var list = new List<RedisObject>(response.Length);
                result.m_List = new ReadOnlyCollection<RedisObject>(list);

                if (response.Length > 0)
                {
                    var items = response.Items;
                    if (items != null)
                    {
                        foreach (var item in items)
                        {
                            if (item != null)
                            {
                                var child = ToObject(item);
                                if (child != null)
                                    list.Add(child);
                            }
                        }
                    }
                }
            }
            return result;
        }

		#endregion Methods
	}
}
