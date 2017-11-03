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

namespace Sweet.Redis
{
    public static class RedisCommandUtils
    {
        public static RedisScanBytes ToScanBytes(RedisRaw array)
        {
            if (array == null)
                return new RedisScanBytes(null);

            var value = array.Value;
            if (value == null)
                return new RedisScanBytes(null);

            var items = value.Items;
            if (items == null)
                return new RedisScanBytes(null);

            var count = items.Count;
            if (count == 0)
                return new RedisScanBytes(null);

            var item = items[0];
            if (item == null)
                return new RedisScanBytes(null);

            if (item.Type != RedisRawObjectType.BulkString)
                throw new RedisException("Invalid scan cursor type");

            var data = item.Data;
            if (data == null || !(data is string))
                throw new RedisException("Invalid scan cursor type");

            var str = data as string;
            if (str.IsEmpty())
                throw new RedisException("Invalid scan cursor type");

            var cursor = ulong.Parse(str);

            var result = (RedisByteArray[])null;
            if (items.Count > 1)
            {
                item = items[1];
                if (item != null)
                {
                    if (item.Type != RedisRawObjectType.Array)
                        throw new RedisException("Invalid scan result type");

                    var subItems = item.Items;
                    if (subItems != null)
                    {
                        var subCount = subItems.Count;
                        if (subCount > 0)
                        {
                            var list = new List<RedisByteArray>(subCount);
                            for (var i = 0; i < count; i++)
                            {
                                var subItem = subItems[i];
                                if (subItem == null || subItem.Type != RedisRawObjectType.BulkString)
                                    throw new RedisException("Invalid scan result item type");

                                list.Add(subItem.Data as byte[]);
                            }

                            result = list.ToArray();
                        }
                    }
                }
            }

            return new RedisScanBytes(new RedisScanBytesData(cursor, result));
        }

        public static RedisScanStrings ToScanStrings(RedisRaw array)
        {
            if (array == null)
                return new RedisScanStrings(null);

            var value = array.Value;
            if (value == null)
                return new RedisScanStrings(null);

            var items = value.Items;
            if (items == null)
                return new RedisScanStrings(null);

            var count = items.Count;
            if (count == 0)
                return new RedisScanStrings(null);

            var item = items[0];
            if (item == null)
                return new RedisScanStrings(null);

            if (item.Type != RedisRawObjectType.BulkString)
                throw new RedisException("Invalid scan cursor type");

            var data = item.DataText;
            if (data.IsEmpty())
                throw new RedisException("Invalid scan cursor type");

            var cursor = ulong.Parse(data);

            var result = (string[])null;
            if (items.Count > 1)
            {
                item = items[1];
                if (item != null)
                {
                    if (item.Type != RedisRawObjectType.Array)
                        throw new RedisException("Invalid scan result type");

                    var subItems = item.Items;
                    if (subItems != null)
                    {
                        var subCount = subItems.Count;
                        if (subCount > 0)
                        {
                            var list = new List<string>(subCount);
                            for (var i = 0; i < count; i++)
                            {
                                var subItem = subItems[i];
                                if (subItem == null || subItem.Type != RedisRawObjectType.BulkString)
                                    throw new RedisException("Invalid scan result item type");

                                list.Add(subItem.DataText);
                            }

                            result = list.ToArray();
                        }
                    }
                }
            }

            return new RedisScanStrings(new RedisScanStringsData(cursor, result));
        }

        public static RedisResult<RedisGeoPosition[]> ToGeoPosition(RedisRaw array)
        {
            if (array == null)
                return new RedisResult<RedisGeoPosition[]>(new RedisGeoPosition[0]);

            var value = array.Value;
            if (value == null)
                return new RedisResult<RedisGeoPosition[]>(new RedisGeoPosition[0]);

            var items = value.Items;
            if (items == null)
                return new RedisResult<RedisGeoPosition[]>(new RedisGeoPosition[0]);

            var count = items.Count;

            var result = new RedisGeoPosition[count];
            if (count > 0)
                for (var i = 0; i < count; i++)
                    result[i] = ToGeoPosition(items[i]);

            return new RedisResult<RedisGeoPosition[]>(result);
        }

        public static RedisResult<RedisGeoRadiusResult[]> ToGeoRadiusArray(RedisRaw array)
        {
            if (array == null)
                return new RedisResult<RedisGeoRadiusResult[]>(new RedisGeoRadiusResult[0]);

            var value = array.Value;
            if (value == null || value.Type != RedisRawObjectType.Array)
                return new RedisResult<RedisGeoRadiusResult[]>(new RedisGeoRadiusResult[0]);

            var items = value.Items;
            if (items == null)
                return new RedisResult<RedisGeoRadiusResult[]>(new RedisGeoRadiusResult[0]);

            var count = items.Count;
            if (count == 0)
                return new RedisResult<RedisGeoRadiusResult[]>(new RedisGeoRadiusResult[0]);

            var list = new List<RedisGeoRadiusResult>(count);
            for (var i = 0; i < count; i++)
                list.Add(ToGeoRadiusResult(items[i]));

            return new RedisResult<RedisGeoRadiusResult[]>(list.ToArray());
        }

        public static RedisGeoRadiusResult ToGeoRadiusResult(RedisRawObject obj)
        {
            if (obj != null)
            {
                if (obj.Type == RedisRawObjectType.BulkString)
                {
                    var member = obj.DataText;
                    if (member != null)
                        return new RedisGeoRadiusResult(member);
                }
                else if (obj.Type == RedisRawObjectType.Array)
                {
                    var items = obj.Items;
                    if (items == null)
                        return null;

                    var count = items.Count;
                    if (count < 1)
                        return null;

                    object data;
                    string member = null;
                    double? distance = null;
                    long? hash = null;
                    RedisGeoPosition? coord = null;

                    var item = items[0];
                    if (item != null && item.Type == RedisRawObjectType.BulkString)
                        member = item.DataText;

                    for (var i = 1; i < count; i++)
                    {
                        var child = items[i];
                        if (child != null)
                        {
                            if (child.Type == RedisRawObjectType.Array)
                                coord = ToGeoPosition(child);
                            else
                            {
                                if (child.Type == RedisRawObjectType.Integer)
                                {
                                    data = child.Data;
                                    if (data is long)
                                        hash = (long)data;
                                }
                                else if (child.Type == RedisRawObjectType.BulkString)
                                {
                                    var str = child.DataText;
                                    if (str != null)
                                    {
                                        var d = 0d;
                                        if (double.TryParse(str, out d))
                                            distance = d;
                                    }
                                }
                            }
                        }
                    }

                    return new RedisGeoRadiusResult(member, coord, distance, hash);
                }
            }
            return null;
        }

        private static RedisGeoPosition ToGeoPosition(RedisRawObject obj)
        {
            if (obj != null && obj.Type == RedisRawObjectType.Array)
            {
                var items = obj.Items;
                if (items != null && items.Count >= 2)
                {
                    var item = items[0];
                    if (item != null && item.Type == RedisRawObjectType.BulkString)
                    {
                        var data = item.DataText;
                        if (!data.IsEmpty())
                        {
                            double longitude;
                            if (double.TryParse(data, out longitude))
                            {
                                item = items[1];
                                if (item != null && item.Type == RedisRawObjectType.BulkString)
                                {
                                    data = item.DataText;
                                    if (!data.IsEmpty())
                                    {
                                        double latitude;
                                        if (double.TryParse(data, out latitude))
                                            return new RedisGeoPosition(longitude, latitude);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return RedisGeoPosition.Empty;
        }
    }
}
