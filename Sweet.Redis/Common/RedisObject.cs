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
            TypeByte = type.ResponseTypeByte();
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

        public int TypeByte { get; private set; }

        #endregion Properties

        #region Methods

        public override string ToString()
        {
            var sBuilder = new StringBuilder();
            Write(this, sBuilder, -1);
            return sBuilder.ToString();
        }

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
            result.TypeByte = response.TypeByte;

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

        private static void Write(RedisObject obj, StringBuilder sBuilder, int indent = 0, int number = 0)
        {
            var indentStr = new string(' ', Math.Max(0, 2 * indent));
            sBuilder.Append(indentStr);

            if (number > 0)
            {
                sBuilder.Append(number);
                sBuilder.Append(") ");
            }

            if (obj == null)
            {
                sBuilder.AppendLine("(nil)");
                return;
            }

            var data = obj.Data;

            switch (obj.Type)
            {
                case RedisObjectType.SimpleString:
                case RedisObjectType.BulkString:
                    {
                        var str = data as string;
                        if (str == null)
                            sBuilder.AppendLine("(nil)");
                        else if (str == String.Empty)
                            sBuilder.AppendLine("(empty)");
                        else
                        {
                            sBuilder.Append('"');
                            sBuilder.Append(str);
                            sBuilder.Append('"');
                            sBuilder.AppendLine();
                        }
                    }
                    break;
                case RedisObjectType.Error:
                    {
                        sBuilder.Append("(error) ");

                        var str = data as string;
                        if (String.IsNullOrEmpty(str))
                            sBuilder.AppendLine("(nil)");
                        else
                        {
                            sBuilder.Append('"');
                            sBuilder.Append(str);
                            sBuilder.Append('"');
                            sBuilder.AppendLine();
                        }
                    }
                    break;
                case RedisObjectType.Integer:
                    {
                        sBuilder.Append("(integer) ");

                        object l = null;
                        if (data is long || data is double)
                            l = data;

                        if (l == null)
                            sBuilder.AppendLine("(nil)");
                        else
                        {
                            sBuilder.Append(l);
                            sBuilder.AppendLine();
                        }
                    }
                    break;
                case RedisObjectType.Array:
                    {
                        if (obj.Count == 0)
                            sBuilder.AppendLine("(empty list or set)");
                        else
                        {
                            var items = obj.Items;
                            if (items == null || items.Count == 0)
                                sBuilder.AppendLine("(empty list or set)");
                            else
                            {
                                var length = items.Count;
                                for (var i = 0; i < length; i++)
                                    Write(items[i], sBuilder, indent + 1, i + 1);
                            }
                        }
                    }
                    break;
                default:
                    sBuilder.AppendFormat("(Unknown reply type: {0})", obj.TypeByte);
                    sBuilder.AppendLine();
                    break;
            }
        }
        #endregion Methods
    }
}
