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
using System.Text;

namespace Sweet.Redis
{
    internal class RedisSortedSetsCommands : RedisCommandSet, IRedisSortedSetsCommands
    {
        #region .Ctors

        public RedisSortedSetsCommands(RedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisDouble ZAdd(RedisParam key, int score, RedisParam member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<int, RedisParam>[] scoresAndMembers)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            var keyBytes = key.Data;

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommandList.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommandList.NX);
                        break;
                    default:
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommandList.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommandList.Incr);

                parameters = parameters.Join(score.ToBytes())
                                       .Join(member);

                if (samsLength > 0)
                {
                    var sams = new byte[2 * samsLength][];

                    for (int i = 0, index = 0; i < samsLength; i++, index += 2)
                    {
                        sams[index] = scoresAndMembers[i].Key.ToBytes();
                        sams[index + 1] = scoresAndMembers[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(sams);
                }

                return ExpectDouble(RedisCommandList.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommandList.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisDouble ZAdd(RedisParam key, long score, RedisParam member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<long, RedisParam>[] scoresAndMembers)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            var keyBytes = key.Data;

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommandList.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommandList.NX);
                        break;
                    default:
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommandList.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommandList.Incr);

                parameters = parameters.Join(score.ToBytes())
                                       .Join(member);

                if (samsLength > 0)
                {
                    var sams = new byte[2 * samsLength][];

                    for (int i = 0, index = 0; i < samsLength; i++, index += 2)
                    {
                        sams[index] = scoresAndMembers[i].Key.ToBytes();
                        sams[index + 1] = scoresAndMembers[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(sams);
                }

                return ExpectDouble(RedisCommandList.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommandList.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisDouble ZAdd(RedisParam key, double score, RedisParam member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<double, RedisParam>[] scoresAndMembers)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            var keyBytes = key.Data;

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommandList.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommandList.NX);
                        break;
                    default:
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommandList.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommandList.Incr);

                parameters = parameters.Join(score.ToBytes())
                                       .Join(member);

                if (samsLength > 0)
                {
                    var sams = new byte[2 * samsLength][];

                    for (int i = 0, index = 0; i < samsLength; i++, index += 2)
                    {
                        sams[index] = scoresAndMembers[i].Key.ToBytes();
                        sams[index + 1] = scoresAndMembers[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(sams);
                }

                return ExpectDouble(RedisCommandList.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommandList.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisInteger ZCard(RedisParam key)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZCard, key);
        }

        public RedisInteger ZCount(RedisParam key, int min, int max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZCount, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZCount(RedisParam key, long min, long max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZCount, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZCount(RedisParam key, double min, double max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZCount, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZCount(RedisParam key, RedisParam min, RedisParam max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZCount, key, min, max);
        }

        public RedisDouble ZIncrBy(RedisParam key, double increment, RedisParam member)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (member.IsNull)
                throw new ArgumentNullException("member");

            return ExpectInteger(RedisCommandList.ZIncrBy, key, increment.ToBytes(), member);
        }

        public RedisDouble ZIncrBy(RedisParam key, int increment, RedisParam member)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (member.IsNull)
                throw new ArgumentNullException("member");

            return ExpectInteger(RedisCommandList.ZIncrBy, key, increment.ToBytes(), member);
        }

        public RedisDouble ZIncrBy(RedisParam key, long increment, RedisParam member)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (member.IsNull)
                throw new ArgumentNullException("member");

            return ExpectInteger(RedisCommandList.ZIncrBy, key, increment.ToBytes(), member);
        }

        public RedisInteger ZInterStore(RedisParam destination, int numkeys, RedisParam key, int weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<RedisParam, int>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination.IsNull)
                throw new ArgumentNullException("destination");

            if (key.IsNull)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.Data;

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key)
                    .Join(weight.ToBytes());

                if (kawsLength > 0)
                {
                    var kaws = new byte[2 * kawsLength][];

                    for (int i = 0, index = 0; i < kawsLength; i++, index += 2)
                    {
                        kaws[index] = keysAndWeight[i].Key.Data;
                        kaws[index + 1] = keysAndWeight[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(kaws);
                }

                switch (aggregate)
                {
                    case RedisAggregate.Sum:
                        parameters = parameters.Join(RedisCommandList.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommandList.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommandList.Max);
                        break;
                    default:
                        break;
                }

                return ExpectInteger(RedisCommandList.ZInterStore, parameters);
            }

            return ExpectInteger(RedisCommandList.ZInterStore, destinationBytes, numkeys.ToBytes(), key, weight.ToBytes());
        }

        public RedisInteger ZInterStore(RedisParam destination, int numkeys, RedisParam key, long weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<RedisParam, long>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination.IsNull)
                throw new ArgumentNullException("destination");

            if (key.IsNull)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.Data;

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key)
                    .Join(weight.ToBytes());

                if (kawsLength > 0)
                {
                    var kaws = new byte[2 * kawsLength][];

                    for (int i = 0, index = 0; i < kawsLength; i++, index += 2)
                    {
                        kaws[index] = keysAndWeight[i].Key.Data;
                        kaws[index + 1] = keysAndWeight[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(kaws);
                }

                switch (aggregate)
                {
                    case RedisAggregate.Sum:
                        parameters = parameters.Join(RedisCommandList.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommandList.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommandList.Max);
                        break;
                    default:
                        break;
                }

                return ExpectInteger(RedisCommandList.ZInterStore, parameters);
            }

            return ExpectInteger(RedisCommandList.ZInterStore, destinationBytes, numkeys.ToBytes(), key, weight.ToBytes());
        }

        public RedisInteger ZInterStore(RedisParam destination, int numkeys, RedisParam key, double weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<RedisParam, double>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination.IsNull)
                throw new ArgumentNullException("destination");

            if (key.IsNull)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.Data;

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key)
                    .Join(weight.ToBytes());

                if (kawsLength > 0)
                {
                    var kaws = new byte[2 * kawsLength][];

                    for (int i = 0, index = 0; i < kawsLength; i++, index += 2)
                    {
                        kaws[index] = keysAndWeight[i].Key.Data;
                        kaws[index + 1] = keysAndWeight[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(kaws);
                }

                switch (aggregate)
                {
                    case RedisAggregate.Sum:
                        parameters = parameters.Join(RedisCommandList.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommandList.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommandList.Max);
                        break;
                    default:
                        break;
                }

                return ExpectInteger(RedisCommandList.ZInterStore, parameters);
            }

            return ExpectInteger(RedisCommandList.ZInterStore, destinationBytes, numkeys.ToBytes(), key, weight.ToBytes());
        }

        public RedisInteger ZLexCount(RedisParam key, double min, double max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZLexCount, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZLexCount(RedisParam key, int min, int max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZLexCount, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZLexCount(RedisParam key, long min, long max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZLexCount, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZLexCount(RedisParam key, RedisParam min, RedisParam max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommandList.ZLexCount, key, min, max);
        }

        public RedisMultiBytes ZRange(RedisParam key, double start, double stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommandList.ZRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRange(RedisParam key, int start, int stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommandList.ZRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRange(RedisParam key, long start, long stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataBytes(RedisCommandList.ZRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRange(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            if (stop.IsEmpty)
                return ExpectMultiDataBytes(RedisCommandList.ZRange, key, start);

            return ExpectMultiDataBytes(RedisCommandList.ZRange, key, start, stop);
        }

        public RedisMultiBytes ZRangeByLex(RedisParam key, double start, double stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByLex(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByLex(RedisParam key, int start, int stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByLex(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByLex(RedisParam key, long start, long stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByLex(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByLex(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataBytes(RedisCommandList.ZRangeByLex, parameters);
        }

        public RedisMultiString ZRangeByLexString(RedisParam key, double start, double stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByLexString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByLexString(RedisParam key, int start, int stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByLexString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByLexString(RedisParam key, long start, long stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByLexString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByLexString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataStrings(RedisCommandList.ZRangeByLex, parameters);
        }

        public RedisMultiBytes ZRangeByScore(RedisParam key, double start, double stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByScore(RedisParam key, int start, int stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByScore(RedisParam key, long start, long stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByScore(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataBytes(RedisCommandList.ZRangeByScore, parameters);
        }

        public RedisMultiString ZRangeByScoreString(RedisParam key, double start, double stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByScoreString(RedisParam key, int start, int stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByScoreString(RedisParam key, long start, long stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByScoreString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataStrings(RedisCommandList.ZRangeByScore, parameters);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(RedisParam key, double start, double stop,
            int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(RedisParam key, int start, int stop,
            int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(RedisParam key, long start, long stop,
            int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(RedisParam key, RedisParam start, RedisParam stop,
            int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            parameters = parameters.Join(RedisCommandList.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommandList.ZRangeByScore, parameters);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<byte[], double>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var d = 0d;
                        var k = bytes[index];

                        if (index < bLength - 1)
                        {
                            var b = bytes[index + 1];
                            if (!b.IsEmpty())
                                double.TryParse(b.ToUTF8String(), out d);
                        }

                        result[i] = new RedisKeyValue<byte[], double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(RedisParam key, double start, double stop,
            int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(RedisParam key, int start, int stop,
            int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(RedisParam key, long start, long stop,
            int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(RedisParam key, RedisParam start, RedisParam stop,
            int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            parameters = parameters.Join(RedisCommandList.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommandList.ZRangeByScore, parameters);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<string, double>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var d = 0d;
                        var k = String.Empty;

                        var b = bytes[index];
                        if (!b.IsEmpty())
                            k = b.ToUTF8String();

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (!b.IsEmpty())
                                double.TryParse(b.ToUTF8String(), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisMultiString ZRangeString(RedisParam key, double start, double stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommandList.ZRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRangeString(RedisParam key, int start, int stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommandList.ZRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRangeString(RedisParam key, long start, long stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ExpectMultiDataStrings(RedisCommandList.ZRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRangeString(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            if (stop.IsEmpty)
                return ExpectMultiDataStrings(RedisCommandList.ZRange, key, start);

            return ExpectMultiDataStrings(RedisCommandList.ZRange, key, start, stop);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(RedisParam key, double start, double stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(RedisParam key, int start, int stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(RedisParam key, long start, long stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            parameters = parameters.Join(RedisCommandList.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommandList.ZRange, parameters);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<byte[], double>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var d = 0d;
                        var k = bytes[index];

                        if (index < bLength - 1)
                        {
                            var b = bytes[index + 1];
                            if (!b.IsEmpty())
                                double.TryParse(b.ToUTF8String(), out d);
                        }

                        result[i] = new RedisKeyValue<byte[], double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(RedisParam key, double start, double stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(RedisParam key, int start, int stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(RedisParam key, long start, long stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            return ZRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            parameters = parameters.Join(RedisCommandList.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommandList.ZRange, parameters);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<string, double>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var d = 0d;
                        var k = String.Empty;

                        var b = bytes[index];
                        if (!b.IsEmpty())
                            k = b.ToUTF8String();

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (!b.IsEmpty())
                                double.TryParse(b.ToUTF8String(), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisNullableInteger ZRank(RedisParam key, RedisParam member)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (member.IsNull)
                throw new ArgumentNullException("member");

            return ExpectNullableInteger(RedisCommandList.ZRank, key, member);
        }

        public RedisInteger ZRem(RedisParam key, RedisParam member, params RedisParam[] members)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (member.IsNull)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            var length = members.Length;
            if (length > 0)
            {
                var parameters = key.Join(member).Join(members);
                return ExpectInteger(RedisCommandList.ZRem, parameters);
            }

            return ExpectInteger(RedisCommandList.ZRem, key, member);
        }

        public RedisInteger ZRemRangeByLex(RedisParam key, double min, double max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectInteger(RedisCommandList.ZRemRangeByLex, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZRemRangeByLex(RedisParam key, int min, int max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectInteger(RedisCommandList.ZRemRangeByLex, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZRemRangeByLex(RedisParam key, long min, long max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectInteger(RedisCommandList.ZRemRangeByLex, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZRemRangeByLex(RedisParam key, RedisParam min, RedisParam max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (min.IsEmpty)
                throw new ArgumentNullException("min");

            ValidateNotDisposed();

            if (!max.IsEmpty)
                return ExpectInteger(RedisCommandList.ZRemRangeByLex, key, min, max);

            return ExpectInteger(RedisCommandList.ZRemRangeByLex, key, min);
        }

        public RedisInteger ZRemRangeByRank(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            if (!stop.IsEmpty)
                return ExpectInteger(RedisCommandList.ZRemRangeByRank, key, start, stop);

            return ExpectInteger(RedisCommandList.ZRemRangeByRank, key, start);
        }

        public RedisInteger ZRemRangeByScore(RedisParam key, double min, double max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectInteger(RedisCommandList.ZRemRangeByScore, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZRemRangeByScore(RedisParam key, int min, int max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectInteger(RedisCommandList.ZRemRangeByScore, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZRemRangeByScore(RedisParam key, long min, long max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectInteger(RedisCommandList.ZRemRangeByScore, key, min.ToBytes(), max.ToBytes());
        }

        public RedisInteger ZRemRangeByScore(RedisParam key, RedisParam min, RedisParam max)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (min.IsEmpty)
                throw new ArgumentNullException("min");

            ValidateNotDisposed();

            if (!max.IsEmpty)
                return ExpectInteger(RedisCommandList.ZRemRangeByScore, key, min, max);

            return ExpectInteger(RedisCommandList.ZRemRangeByScore, key, min);
        }

        public RedisMultiBytes ZRevRange(RedisParam key, double start, double stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectMultiDataBytes(RedisCommandList.ZRevRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRevRange(RedisParam key, int start, int stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectMultiDataBytes(RedisCommandList.ZRevRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRevRange(RedisParam key, long start, long stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();
            return ExpectMultiDataBytes(RedisCommandList.ZRevRange, key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRevRange(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            if (!stop.IsEmpty)
                return ExpectMultiDataBytes(RedisCommandList.ZRevRange, key, start, stop);

            return ExpectMultiDataBytes(RedisCommandList.ZRevRange, key, start);
        }

        public RedisMultiBytes ZRevRangeByScore(RedisParam key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRevRangeByScore(RedisParam key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRevRangeByScore(RedisParam key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRevRangeByScore(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataBytes(RedisCommandList.ZRevRangeByScore, parameters);
        }

        public RedisMultiString ZRevRangeByScoreString(RedisParam key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRevRangeByScoreString(RedisParam key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRevRangeByScoreString(RedisParam key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRevRangeByScoreString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataStrings(RedisCommandList.ZRevRangeByScore, parameters);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(RedisParam key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(RedisParam key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(RedisParam key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            parameters = parameters.Join(RedisCommandList.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommandList.ZRevRangeByScore, parameters);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<byte[], double>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var d = 0d;
                        var k = bytes[index];

                        if (index < bLength - 1)
                        {
                            var b = bytes[index + 1];
                            if (!b.IsEmpty())
                                double.TryParse(b.ToUTF8String(), out d);
                        }

                        result[i] = new RedisKeyValue<byte[], double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(RedisParam key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(RedisParam key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(RedisParam key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(RedisParam key, RedisParam start, RedisParam stop, int? offset = null, int? count = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommandList.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            parameters = parameters.Join(RedisCommandList.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommandList.ZRevRangeByScore, parameters);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<string, double>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var d = 0d;
                        var k = String.Empty;

                        var b = bytes[index];
                        if (!b.IsEmpty())
                            k = b.ToUTF8String();

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (!b.IsEmpty())
                                double.TryParse(b.ToUTF8String(), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisMultiString ZRevRangeString(RedisParam key, double start, double stop)
        {
            return ZRevRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRevRangeString(RedisParam key, int start, int stop)
        {
            return ZRevRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRevRangeString(RedisParam key, long start, long stop)
        {
            return ZRevRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRevRangeString(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            if (!stop.IsEmpty)
                return ExpectMultiDataStrings(RedisCommandList.ZRevRange, key, start, stop);

            return ExpectMultiDataStrings(RedisCommandList.ZRevRange, key, start);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(RedisParam key, double start, double stop)
        {
            return ZRevRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(RedisParam key, int start, int stop)
        {
            return ZRevRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(RedisParam key, long start, long stop)
        {
            return ZRevRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            parameters = parameters.Join(RedisCommandList.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommandList.ZRevRange, parameters);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<byte[], double>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var d = 0d;
                        var k = bytes[index];

                        if (index < bLength - 1)
                        {
                            var b = bytes[index + 1];
                            if (!b.IsEmpty())
                                double.TryParse(b.ToUTF8String(), out d);
                        }

                        result[i] = new RedisKeyValue<byte[], double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(RedisParam key, double start, double stop)
        {
            return ZRevRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(RedisParam key, int start, int stop)
        {
            return ZRevRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(RedisParam key, long start, long stop)
        {
            return ZRevRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(RedisParam key, RedisParam start, RedisParam stop)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (start.IsEmpty)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.Join(start);
            if (!stop.IsEmpty)
                parameters = parameters.Join(stop);

            parameters = parameters.Join(RedisCommandList.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommandList.ZRevRange, parameters);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<string, double>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var d = 0d;
                        var k = String.Empty;

                        var b = bytes[index];
                        if (!b.IsEmpty())
                            k = b.ToUTF8String();

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (!b.IsEmpty())
                                double.TryParse(b.ToUTF8String(), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisNullableInteger ZRevRank(RedisParam key, RedisParam member)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (member.IsNull)
                throw new ArgumentNullException("member");

            return ExpectNullableInteger(RedisCommandList.ZRevRank, key, member);
        }

        public RedisScanBytes ZScan(RedisParam key, ulong cursor = 0uL, int count = 10, RedisParam? match = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = new byte[][] { key.Data, cursor.ToBytes() };

            if (match.HasValue)
            {
                var value = match.Value;
                if (!value.IsEmpty)
                {
                    parameters = parameters.Join(RedisCommandList.Match);
                    parameters = parameters.Join(value.Data);
                }
            }

            if (count > 0)
            {
                parameters = parameters.Join(RedisCommandList.Count);
                parameters = parameters.Join(count.ToBytes());
            }

            return RedisCommandUtils.ToScanBytes(ExpectArray(RedisCommandList.ZScan, parameters));
        }

        public RedisScanStrings ZScanString(RedisParam key, ulong cursor = 0uL, int count = 10, RedisParam? match = null)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            var parameters = new byte[][] { key.Data, cursor.ToBytes() };

            if (match.HasValue)
            {
                var value = match.Value;
                if (!value.IsEmpty)
                {
                    parameters = parameters.Join(RedisCommandList.Match);
                    parameters = parameters.Join(value.Data);
                }
            }

            if (count > 0)
            {
                parameters = parameters.Join(RedisCommandList.Count);
                parameters = parameters.Join(count.ToBytes());
            }

            return RedisCommandUtils.ToScanStrings(ExpectArray(RedisCommandList.ZScan, parameters));
        }

        public RedisDouble ZScore(RedisParam key, RedisParam member)
        {
            if (key.IsNull)
                throw new ArgumentNullException("key");

            if (member.IsNull)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            return ExpectDouble(RedisCommandList.ZScore, key, member.ToBytes());
        }

        public RedisInteger ZUnionStore(RedisParam destination, int numkeys, RedisParam key, int weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<RedisParam, int>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination.IsEmpty)
                throw new ArgumentNullException("destination");

            if (key.IsNull)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.Data;

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key)
                    .Join(weight.ToBytes());

                if (kawsLength > 0)
                {
                    var kaws = new byte[2 * kawsLength][];

                    for (int i = 0, index = 0; i < kawsLength; i++, index += 2)
                    {
                        kaws[index] = keysAndWeight[i].Key.ToBytes();
                        kaws[index + 1] = keysAndWeight[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(kaws);
                }

                switch (aggregate)
                {
                    case RedisAggregate.Sum:
                        parameters = parameters.Join(RedisCommandList.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommandList.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommandList.Max);
                        break;
                    default:
                        break;
                }

                return ExpectInteger(RedisCommandList.ZUnionStore, parameters);
            }

            return ExpectInteger(RedisCommandList.ZUnionStore, destinationBytes, numkeys.ToBytes(), key, weight.ToBytes());
        }

        public RedisInteger ZUnionStore(RedisParam destination, int numkeys, RedisParam key, long weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<RedisParam, long>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination.IsEmpty)
                throw new ArgumentNullException("destination");

            if (key.IsNull)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.Data;

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key)
                    .Join(weight.ToBytes());

                if (kawsLength > 0)
                {
                    var kaws = new byte[2 * kawsLength][];

                    for (int i = 0, index = 0; i < kawsLength; i++, index += 2)
                    {
                        kaws[index] = keysAndWeight[i].Key.ToBytes();
                        kaws[index + 1] = keysAndWeight[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(kaws);
                }

                switch (aggregate)
                {
                    case RedisAggregate.Sum:
                        parameters = parameters.Join(RedisCommandList.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommandList.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommandList.Max);
                        break;
                    default:
                        break;
                }

                return ExpectInteger(RedisCommandList.ZUnionStore, parameters);
            }

            return ExpectInteger(RedisCommandList.ZUnionStore, destinationBytes, numkeys.ToBytes(), key, weight.ToBytes());
        }

        public RedisInteger ZUnionStore(RedisParam destination, int numkeys, RedisParam key, double weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<RedisParam, double>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination.IsEmpty)
                throw new ArgumentNullException("destination");

            if (key.IsNull)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.Data;

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key)
                    .Join(weight.ToBytes());

                if (kawsLength > 0)
                {
                    var kaws = new byte[2 * kawsLength][];

                    for (int i = 0, index = 0; i < kawsLength; i++, index += 2)
                    {
                        kaws[index] = keysAndWeight[i].Key.ToBytes();
                        kaws[index + 1] = keysAndWeight[i].Value.ToBytes();
                    }

                    parameters = parameters.Join(kaws);
                }

                switch (aggregate)
                {
                    case RedisAggregate.Sum:
                        parameters = parameters.Join(RedisCommandList.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommandList.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommandList.Max);
                        break;
                    default:
                        break;
                }

                return ExpectInteger(RedisCommandList.ZUnionStore, parameters);
            }

            return ExpectInteger(RedisCommandList.ZUnionStore, destinationBytes, numkeys.ToBytes(), key, weight.ToBytes());
        }

        #endregion Methods
    }
}
