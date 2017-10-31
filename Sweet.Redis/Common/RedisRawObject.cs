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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace Sweet.Redis
{
    public class RedisRawObject
    {
        #region Field Members

        private ReadOnlyCollection<RedisRawObject> m_List;

        #endregion Field Members

        #region .Ctors

        public RedisRawObject(RedisRawObjectType type, object data)
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
                if (Type != RedisRawObjectType.Array)
                    return -1;

                var list = m_List;
                return (list != null) ? list.Count : 0;
            }
        }

        public object Data { get; private set; }

        public string DataText
        {
            get
            {
                var data = Data;
                if (!ReferenceEquals(data, null))
                {
                    switch (Type)
                    {
                        case RedisRawObjectType.BulkString:
                            if (data is byte[])
                                return Encoding.UTF8.GetString((byte[])data);
                            return data as string;
                        case RedisRawObjectType.SimpleString:
                        case RedisRawObjectType.Error:
                            if (data is string)
                                return (string)data;
                            if (data is byte[])
                                return Encoding.UTF8.GetString((byte[])data);
                            break;
                        case RedisRawObjectType.Integer:
                            if (data is long)
                                return ((long)data).ToString(RedisConstants.InvariantCulture);
                            if (data is double)
                                return ((double)data).ToString(RedisConstants.InvariantCulture);
                            break;
                        default:
                            break;
                    }
                }
                return null;
            }
        }

        public IList<RedisRawObject> Items
        {
            get { return m_List; }
        }

        public RedisRawObjectType Type { get; private set; }

        public int TypeByte { get; private set; }

        #endregion Properties

        #region Methods

        public override string ToString()
        {
            var sBuilder = new StringBuilder();
            Write(this, sBuilder, -1);
            return sBuilder.ToString();
        }

        public static RedisRawObject ToObject(IRedisRawResponse response)
        {
            if (response == null)
                return null;

            var type = response.Type;
            if (type == RedisRawObjectType.Undefined)
                throw new RedisException("Undefined redis response", RedisErrorCode.CorruptResponse);

            object data = null;
            var bytes = response.Data;

            if (bytes != null)
            {
                switch (type)
                {
                    case RedisRawObjectType.SimpleString:
                    case RedisRawObjectType.Error:
                        data = Encoding.UTF8.GetString(bytes);
                        break;
                    case RedisRawObjectType.BulkString:
                        data = bytes;
                        break;
                    case RedisRawObjectType.Integer:
                        if (bytes.Length == 0)
                            throw new RedisException("Invalid integer value", RedisErrorCode.CorruptResponse);

                        long l;
                        if (!long.TryParse(Encoding.UTF8.GetString(bytes), out l))
                            throw new RedisException("Invalid integer value", RedisErrorCode.CorruptResponse);

                        data = l;
                        break;
                    default:
                        break;
                }
            }

            var result = new RedisRawObject(type, data);
            result.TypeByte = response.TypeByte;

            if (type == RedisRawObjectType.Array && response.Length > -1)
            {
                var list = new List<RedisRawObject>(response.Length);
                result.m_List = new ReadOnlyCollection<RedisRawObject>(list);

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

        private static void Write(RedisRawObject obj, StringBuilder sBuilder, int indent = 0, int number = 0)
        {
            var indentStr = new string(' ', Math.Max(0, 2 * indent));
            sBuilder.Append(indentStr);

            if (number > 0)
            {
                sBuilder.Append(number);
                sBuilder.Append(") ");
            }

            if (ReferenceEquals(obj, null))
            {
                sBuilder.AppendLine("(nil)");
                return;
            }

            var data = obj.Data;

            switch (obj.Type)
            {
                case RedisRawObjectType.BulkString:
                    {
                        var str = data as byte[];
                        if (str == null)
                            sBuilder.AppendLine("(nil)");
                        else if (str.Length == 0)
                            sBuilder.AppendLine("(empty)");
                        else
                        {
                            sBuilder.Append('"');
                            sBuilder.Append(Encoding.UTF8.GetString(str));
                            sBuilder.Append('"');
                            sBuilder.AppendLine();
                        }
                    }
                    break;
                case RedisRawObjectType.SimpleString:
                case RedisRawObjectType.Error:
                    {
                        if (obj.Type == RedisRawObjectType.Error)
                            sBuilder.Append("(error) ");

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
                case RedisRawObjectType.Integer:
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
                case RedisRawObjectType.Array:
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
