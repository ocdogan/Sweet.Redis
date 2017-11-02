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

        public RedisGeoCommands(RedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisInteger GeoAdd(RedisParam key, RedisGeospatialItem member, params RedisGeospatialItem[] members)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            if (member.IsEmpty)
                throw new ArgumentNullException("member");

            if (members.IsEmpty())
                return ExpectInteger(RedisCommandList.GeoAdd, key, member.Longitude.ToBytes(),
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

            return ExpectInteger(RedisCommandList.GeoAdd, parameters);
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
                return ExpectNullableDouble(RedisCommandList.GeoDist, key, member1, member2);

            return ExpectNullableDouble(RedisCommandList.GeoDist, key, member1, member2, ToBytes(unit));
        }

        private static byte[] ToBytes(RedisGeoDistanceUnit unit)
        {
            switch (unit)
            {
                case RedisGeoDistanceUnit.Meters:
                    return RedisCommandList.Meters;
                case RedisGeoDistanceUnit.Kilometers:
                    return RedisCommandList.Kilometers;
                case RedisGeoDistanceUnit.Feet:
                    return RedisCommandList.Feet;
                case RedisGeoDistanceUnit.Miles:
                    return RedisCommandList.Miles;
                default:
                    return RedisCommandList.Meters;
            }
        }

        public RedisMultiBytes GeoHash(RedisParam key, RedisParam member, params RedisParam[] members)
        {
            if (key.IsEmpty)
                throw new ArgumentNullException("key");

            if (member.IsEmpty)
                throw new ArgumentNullException("member");

            if (members.IsEmpty())
                return ExpectMultiDataBytes(RedisCommandList.GeoHash, key, member);

            var parameters = key.Join(member);

            foreach (var m in members)
            {
                if (!m.IsEmpty)
                    parameters = parameters.Join(m);
            }

            return ExpectMultiDataBytes(RedisCommandList.GeoHash, parameters);
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

            return RedisCommandUtils.ToGeoPosition(ExpectArray(RedisCommandList.GeoPos, parameters));
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
                parameters = parameters.Join(RedisCommandList.WithCoord);

            if (withDist)
                parameters = parameters.Join(RedisCommandList.WithDist);

            if (withHash)
                parameters = parameters.Join(RedisCommandList.WithHash);

            if (count > -1)
                parameters = parameters.Join(RedisCommandList.Count).Join(count.ToBytes());

            if (sort == RedisSortDirection.Ascending)
                parameters = parameters.Join(RedisCommandList.Ascending);
            else if (sort == RedisSortDirection.Descending)
                parameters = parameters.Join(RedisCommandList.Descending);

            if (storeKey.HasValue && !storeKey.Value.IsEmpty)
                parameters = parameters.Join(RedisCommandList.Store).Join(storeKey.ToBytes());

            if (storeDistanceKey.HasValue && !storeDistanceKey.Value.IsEmpty)
                parameters = parameters.Join(RedisCommandList.StoreDist).Join(storeDistanceKey.ToBytes());

            return RedisCommandUtils.ToGeoRadiusArray(ExpectArray(RedisCommandList.GeoRadius, parameters));
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
                parameters = parameters.Join(RedisCommandList.WithCoord);

            if (withDist)
                parameters = parameters.Join(RedisCommandList.WithDist);

            if (withHash)
                parameters = parameters.Join(RedisCommandList.WithHash);

            if (count > -1)
                parameters = parameters.Join(RedisCommandList.Count).Join(count.ToBytes());

            if (sort == RedisSortDirection.Ascending)
                parameters = parameters.Join(RedisCommandList.Ascending);
            else if (sort == RedisSortDirection.Descending)
                parameters = parameters.Join(RedisCommandList.Descending);

            if (storeKey.HasValue && !storeKey.Value.IsEmpty)
                parameters = parameters.Join(RedisCommandList.Store).Join(storeKey.ToBytes());

            if (storeDistanceKey.HasValue && !storeDistanceKey.Value.IsEmpty)
                parameters = parameters.Join(RedisCommandList.StoreDist).Join(storeDistanceKey.ToBytes());

            return RedisCommandUtils.ToGeoRadiusArray(ExpectArray(RedisCommandList.GeoRadiusByMember, parameters));
        }

        #endregion Methods
    }
}
