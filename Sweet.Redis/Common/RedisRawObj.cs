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
    public class RedisRawObj
    {
        #region Field Members

        private ReadOnlyCollection<RedisRawObj> m_List;

        #endregion Field Members

        #region .Ctors

        public RedisRawObj(RedisRawObjType type, object data)
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
                if (Type != RedisRawObjType.Array)
                    return -1;

                var list = m_List;
                return (list != null) ? list.Count : 0;
            }
        }

        public object Data { get; private set; }

        public IList<RedisRawObj> Items
        {
            get { return m_List; }
        }

        public RedisRawObjType Type { get; private set; }

        public int TypeByte { get; private set; }

        #endregion Properties

        #region Methods

        public override string ToString()
        {
            var sBuilder = new StringBuilder();
            Write(this, sBuilder, -1);
            return sBuilder.ToString();
        }

        public static RedisRawObj ToObject(IRedisResponse response)
        {
            if (response == null)
                return null;

            var type = response.Type;
            if (type == RedisRawObjType.Undefined)
                throw new RedisException("Undefined redis response");

            object data = null;
            var bytes = response.Data;

            if (bytes != null)
            {
                switch (type)
                {
                    case RedisRawObjType.SimpleString:
                    case RedisRawObjType.BulkString:
                    case RedisRawObjType.Error:
                        data = Encoding.UTF8.GetString(bytes);
                        break;
                    case RedisRawObjType.Integer:
                        if (bytes.Length == 0)
                            throw new RedisException("Invalid integer value");

                        long l;
                        if (!long.TryParse(Encoding.UTF8.GetString(bytes), out l))
                            throw new RedisException("Invalid integer value");

                        data = l;
                        break;
                }
            }

            var result = new RedisRawObj(type, data);
            result.TypeByte = response.TypeByte;

            if (type == RedisRawObjType.Array && response.Length > -1)
            {
                var list = new List<RedisRawObj>(response.Length);
                result.m_List = new ReadOnlyCollection<RedisRawObj>(list);

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

        private static void Write(RedisRawObj obj, StringBuilder sBuilder, int indent = 0, int number = 0)
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
                case RedisRawObjType.SimpleString:
                case RedisRawObjType.BulkString:
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
                case RedisRawObjType.Error:
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
                case RedisRawObjType.Integer:
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
                case RedisRawObjType.Array:
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
