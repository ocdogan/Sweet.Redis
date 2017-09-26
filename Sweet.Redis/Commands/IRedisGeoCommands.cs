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

namespace Sweet.Redis
{
    /*
    GEOADD key longitude latitude member [longitude latitude member ...]
    summary: Add one or more geospatial items in the geospatial index represented using a sorted set
    since: 3.2.0

    GEODIST key member1 member2 [unit]
    summary: Returns the distance between two members of a geospatial index
    since: 3.2.0

    GEOHASH key member [member ...]
    summary: Returns members of a geospatial index as standard geohash strings
    since: 3.2.0

    GEOPOS key member [member ...]
    summary: Returns longitude and latitude of members of a geospatial index
    since: 3.2.0

    GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
    summary: Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a point
    since: 3.2.0

    GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]
    summary: Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a member
    since: 3.2.0
    */
    public interface IRedisGeoCommands
    {
        RedisInt GeoAdd(RedisParam key, RedisGeospatialItem member, params RedisGeospatialItem[] members);
        RedisNullableDouble GeoDistance(RedisParam key, RedisParam member1, RedisParam member2, RedisGeoDistanceUnit unit = RedisGeoDistanceUnit.Default);
        RedisMultiBytes GeoHash(RedisParam key, RedisParam member, params RedisParam[] members);
        RedisResult<RedisGeoPosition[]> GeoPosition(RedisParam key, RedisParam member, params RedisParam[] members);
        RedisResult<RedisGeoRadiusResult[]> GeoRadius(RedisParam key, RedisGeoPosition position, double radius,
                       RedisGeoDistanceUnit unit, bool withCoord = false, bool withDist = false, bool withHash = false,
                       int count = -1, RedisSortDirection sort = RedisSortDirection.Default, RedisParam? storeKey = null,
                       RedisParam? storeDistanceKey = null);
        RedisResult<RedisGeoRadiusResult[]> GeoRadiusByMember(RedisParam key, RedisParam member, double radius,
                       RedisGeoDistanceUnit unit, bool withCoord = false, bool withDist = false, bool withHash = false,
                       int count = -1, RedisSortDirection sort = RedisSortDirection.Default, RedisParam? storeKey = null,
                       RedisParam? storeDistanceKey = null);
    }
}
