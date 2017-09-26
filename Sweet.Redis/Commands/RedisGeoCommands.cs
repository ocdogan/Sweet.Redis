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
    internal class RedisGeoCommands : RedisCommandSet, IRedisGeoCommands
    {
        #region .Ctors

        public RedisGeoCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisInt GeoAdd(RedisParam key, RedisGeospatialItem member, params RedisGeospatialItem[] members)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            if (member.IsEmpty)
                throw new ArgumentNullException("member");

            if (members.Length == 0)
                return ExpectInteger(RedisCommands.GeoAdd, key, member.Longitude.ToBytes(),
                                member.Latitude.ToBytes(), member.Name.ToBytes());

            var parameters = key
                                .Join(member.Longitude.ToBytes())
                                .Join(member.Latitude.ToBytes())
                                .Join(member.Name.ToBytes());

            foreach (var m in members)
            {
                parameters = parameters
                                .Join(m.Longitude.ToBytes())
                                .Join(m.Latitude.ToBytes())
                                .Join(m.Name.ToBytes());
            }

            return ExpectInteger(RedisCommands.GeoAdd, parameters);
        }

        public RedisNullableDouble GeoDistance(RedisParam key, RedisParam member1, RedisParam member2, RedisGeoDistanceUnit unit = RedisGeoDistanceUnit.Default)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            if (member1.IsEmpty)
                throw new ArgumentNullException("member1");

            if (member2.IsEmpty)
                throw new ArgumentNullException("member2");

            if (unit == RedisGeoDistanceUnit.Default)
                return ExpectNullableDouble(RedisCommands.GeoDist, key, member1, member2);

            return ExpectNullableDouble(RedisCommands.GeoDist, key, member1, member2, ToBytes(unit));
        }

        private static byte[] ToBytes(RedisGeoDistanceUnit unit)
        {
            switch (unit)
            {
                case RedisGeoDistanceUnit.Meters:
                    return RedisCommands.Meters;
                case RedisGeoDistanceUnit.Kilometers:
                    return RedisCommands.Kilometers;
                case RedisGeoDistanceUnit.Feet:
                    return RedisCommands.Feet;
                case RedisGeoDistanceUnit.Miles:
                    return RedisCommands.Miles;
                default:
                    return RedisCommands.Meters;
            }
        }

        public RedisMultiBytes GeoHash(RedisParam key, RedisParam member, params RedisParam[] members)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            if (member.IsEmpty)
                throw new ArgumentNullException("member");

            if (members.Length == 0)
                return ExpectMultiDataBytes(RedisCommands.GeoHash, key, member);

            var parameters = key.Join(member);

            foreach (var m in members)
            {
                if (!m.IsEmpty)
                    parameters = parameters.Join(m);
            }

            return ExpectMultiDataBytes(RedisCommands.GeoHash, parameters);
        }


        public RedisResult<RedisGeoPosition[]> GeoPosition(RedisParam key, RedisParam member, params RedisParam[] members)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            if (member.IsEmpty)
                throw new ArgumentNullException("member");

            var parameters = key.Join(member);
            foreach (var m in members)
            {
                if (!m.IsEmpty)
                    parameters = parameters.Join(m);
            }

            return ToGeoPosition(ExpectArray(RedisCommands.GeoPos, parameters));
        }

        private static RedisResult<RedisGeoPosition[]> ToGeoPosition(RedisRaw array)
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

        public RedisResult<RedisGeoRadiusResult[]> GeoRadius(RedisParam key, RedisGeoPosition position, double radius,
                RedisGeoDistanceUnit unit, bool withCoord = false, bool withDist = false, bool withHash = false,
                int count = -1, RedisSortDirection sort = RedisSortDirection.Default, RedisParam? storeKey = null,
                RedisParam? storeDistanceKey = null)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            var parameters = key
                                .Join(position.Longitude.ToBytes())
                                .Join(position.Latitude.ToBytes())
                                .Join(radius.ToBytes())
                                .Join(ToBytes(unit));

            if (withCoord)
                parameters = parameters.Join(RedisCommands.WithCoord);

            if (withDist)
                parameters = parameters.Join(RedisCommands.WithDist);

            if (withHash)
                parameters = parameters.Join(RedisCommands.WithHash);

            if (count > -1)
                parameters = parameters.Join(RedisCommands.Count).Join(count.ToBytes());

            if (sort == RedisSortDirection.Ascending)
                parameters = parameters.Join(RedisCommands.Ascending);
            else if (sort == RedisSortDirection.Descending)
                parameters = parameters.Join(RedisCommands.Descending);

            if (storeKey.HasValue && !storeKey.Value.IsEmpty)
                parameters = parameters.Join(RedisCommands.Store).Join(storeKey.ToBytes());

            if (storeDistanceKey.HasValue && !storeDistanceKey.Value.IsEmpty)
                parameters = parameters.Join(RedisCommands.StoreDist).Join(storeDistanceKey.ToBytes());

            return ToGeoRadiusArray(ExpectArray(RedisCommands.GeoRadius, parameters));
        }

        public RedisResult<RedisGeoRadiusResult[]> GeoRadiusByMember(RedisParam key, RedisParam member, double radius,
                RedisGeoDistanceUnit unit, bool withCoord = false, bool withDist = false, bool withHash = false,
                int count = -1, RedisSortDirection sort = RedisSortDirection.Default, RedisParam? storeKey = null,
                RedisParam? storeDistanceKey = null)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            if (member.IsEmpty)
                throw new ArgumentNullException("member");

            var parameters = key
                                .Join(member)
                                .Join(radius.ToBytes())
                                .Join(ToBytes(unit));

            if (withCoord)
                parameters = parameters.Join(RedisCommands.WithCoord);

            if (withDist)
                parameters = parameters.Join(RedisCommands.WithDist);

            if (withHash)
                parameters = parameters.Join(RedisCommands.WithHash);

            if (count > -1)
                parameters = parameters.Join(RedisCommands.Count).Join(count.ToBytes());

            if (sort == RedisSortDirection.Ascending)
                parameters = parameters.Join(RedisCommands.Ascending);
            else if (sort == RedisSortDirection.Descending)
                parameters = parameters.Join(RedisCommands.Descending);

            if (storeKey.HasValue && !storeKey.Value.IsEmpty)
                parameters = parameters.Join(RedisCommands.Store).Join(storeKey.ToBytes());

            if (storeDistanceKey.HasValue && !storeDistanceKey.Value.IsEmpty)
                parameters = parameters.Join(RedisCommands.StoreDist).Join(storeDistanceKey.ToBytes());

            return ToGeoRadiusArray(ExpectArray(RedisCommands.GeoRadiusByMember, parameters));
        }

        private static RedisResult<RedisGeoRadiusResult[]> ToGeoRadiusArray(RedisRaw array)
        {
            if (array == null)
                return new RedisResult<RedisGeoRadiusResult[]>(new RedisGeoRadiusResult[0]);

            var value = array.Value;
            if (value == null || value.Type != RedisRawObjType.Array)
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

        private static RedisGeoRadiusResult ToGeoRadiusResult(RedisRawObj obj)
        {
            if (obj != null)
            {
                if (obj.Type == RedisRawObjType.BulkString)
                {
                    var member = obj.Data as string;
                    if (member != null)
                        return new RedisGeoRadiusResult(member);
                }
                else if (obj.Type == RedisRawObjType.Array)
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
                    if (item != null && item.Type == RedisRawObjType.BulkString)
                    {
                        data = item.Data;
                        if (data != null && data is string)
                            member = (string)data;
                    }

                    for (var i = 1; i < count; i++)
                    {
                        var child = items[i];
                        if (child != null)
                        {
                            data = child.Data;
                            if (child.Type == RedisRawObjType.Array)
                                coord = ToGeoPosition(child);
                            else if (data != null)
                            {
                                if (child.Type == RedisRawObjType.Integer)
                                {
                                    if (data is long)
                                        hash = (long)data;
                                }
                                else if (child.Type == RedisRawObjType.BulkString)
                                {
                                    var str = data as string;
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

        private static RedisGeoPosition ToGeoPosition(RedisRawObj obj)
        {
            if (obj != null && obj.Type == RedisRawObjType.Array)
            {
                var items = obj.Items;
                if (items != null && items.Count >= 2)
                {
                    var item = items[0];
                    if (item != null && item.Type == RedisRawObjType.BulkString)
                    {
                        var data = item.Data;
                        if (data != null && data is string)
                        {
                            double longitude;
                            if (double.TryParse((string)data, out longitude))
                            {
                                item = items[1];
                                if (item != null && item.Type == RedisRawObjType.BulkString)
                                {
                                    data = item.Data;
                                    if (data != null && data is string)
                                    {
                                        double latitude;
                                        if (double.TryParse((string)data, out latitude))
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

        #endregion Methods
    }
}
