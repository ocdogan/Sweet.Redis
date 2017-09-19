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

        public RedisSortedSetsCommands(IRedisDb db)
            : base(db)
        { }

        #endregion .Ctors

        #region Methods

        public RedisDouble ZAdd(string key, int score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<int, string>[] scoresAndMembers)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            var keyBytes = key.ToBytes();

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[1][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommands.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommands.NX);
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommands.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommands.Incr);

                parameters = parameters.Join(score.ToBytes())
                                       .Join(member.ToBytes());

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

                return ExpectDouble(RedisCommands.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommands.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisDouble ZAdd(string key, int score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<int, byte[]>[] scoresAndMembers)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            var keyBytes = key.ToBytes();

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[1][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommands.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommands.NX);
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommands.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommands.Incr);

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

                return ExpectDouble(RedisCommands.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommands.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisDouble ZAdd(string key, long score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<long, string>[] scoresAndMembers)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            var keyBytes = key.ToBytes();

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[1][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommands.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommands.NX);
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommands.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommands.Incr);

                parameters = parameters.Join(score.ToBytes())
                                       .Join(member.ToBytes());

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

                return ExpectDouble(RedisCommands.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommands.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisDouble ZAdd(string key, long score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<long, byte[]>[] scoresAndMembers)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            var keyBytes = key.ToBytes();

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[1][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommands.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommands.NX);
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommands.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommands.Incr);

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

                return ExpectDouble(RedisCommands.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommands.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisDouble ZAdd(string key, double score, string member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<double, string>[] scoresAndMembers)
        {
            ValidateNotDisposed();

            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            var keyBytes = key.ToBytes();

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[1][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommands.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommands.NX);
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommands.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommands.Incr);

                parameters = parameters.Join(score.ToBytes())
                                       .Join(member.ToBytes());

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

                return ExpectDouble(RedisCommands.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommands.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisDouble ZAdd(string key, double score, byte[] member, RedisUpdateOption updateOption = RedisUpdateOption.Default,
                           bool changed = false, bool increment = false, params RedisKeyValue<double, byte[]>[] scoresAndMembers)
        {
            ValidateNotDisposed();
            ValidateKeyAndValue(key, member, valueName: "member");

            var keyBytes = key.ToBytes();

            var samsLength = scoresAndMembers.Length;

            if (updateOption != RedisUpdateOption.Default ||
                changed || increment || samsLength > 0)
            {
                var parameters = new byte[1][] { keyBytes };

                switch (updateOption)
                {
                    case RedisUpdateOption.OnlyExistings:
                        parameters = parameters.Join(RedisCommands.XX);
                        break;
                    case RedisUpdateOption.OnlyNotExistings:
                        parameters = parameters.Join(RedisCommands.NX);
                        break;
                }

                if (changed)
                    parameters = parameters.Join(RedisCommands.CH);

                if (increment)
                    parameters = parameters.Join(RedisCommands.Incr);

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

                return ExpectDouble(RedisCommands.ZAdd, parameters);
            }

            return ExpectDouble(RedisCommands.ZAdd, keyBytes, score.ToBytes(), member.ToBytes());
        }

        public RedisInt ZCard(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.ZCard, key.ToBytes());
        }

        public RedisInt ZCount(string key, int min, int max)
        {
            return ZCount(key, (double)min, (double)max);
        }

        public RedisInt ZCount(string key, long min, long max)
        {
            return ZCount(key, (double)min, (double)max);
        }

        public RedisInt ZCount(string key, double min, double max)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.ZCount, key.ToBytes(), min.ToBytes(), max.ToBytes());
        }

        public RedisDouble ZIncrBy(string key, double increment, string member)
        {
            return ZIncrBy(key, increment, member.ToBytes());
        }

        public RedisDouble ZIncrBy(string key, double increment, byte[] member)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            return ExpectInteger(RedisCommands.ZIncrBy, key.ToBytes(), increment.ToBytes(), member);
        }

        public RedisDouble ZIncrBy(string key, int increment, string member)
        {
            return ZIncrBy(key, (double)increment, member.ToBytes());
        }

        public RedisDouble ZIncrBy(string key, int increment, byte[] member)
        {
            return ZIncrBy(key, (double)increment, member);
        }

        public RedisDouble ZIncrBy(string key, long increment, string member)
        {
            return ZIncrBy(key, (double)increment, member.ToBytes());
        }

        public RedisDouble ZIncrBy(string key, long increment, byte[] member)
        {
            return ZIncrBy(key, (double)increment, member);
        }

        public RedisInt ZInterStore(string destination, int numkeys, string key, int weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<string, int>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination == null)
                throw new ArgumentNullException("destination");

            if (key == null)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.ToBytes();

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[1][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key.ToBytes())
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
                        parameters = parameters.Join(RedisCommands.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommands.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommands.Max);
                        break;
                }

                return ExpectInteger(RedisCommands.ZInterStore, parameters);
            }

            return ExpectInteger(RedisCommands.ZInterStore, destinationBytes, numkeys.ToBytes(), key.ToBytes(), weight.ToBytes());
        }

        public RedisInt ZInterStore(string destination, int numkeys, string key, long weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<string, long>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination == null)
                throw new ArgumentNullException("destination");

            if (key == null)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.ToBytes();

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[1][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key.ToBytes())
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
                        parameters = parameters.Join(RedisCommands.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommands.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommands.Max);
                        break;
                }

                return ExpectInteger(RedisCommands.ZInterStore, parameters);
            }

            return ExpectInteger(RedisCommands.ZInterStore, destinationBytes, numkeys.ToBytes(), key.ToBytes(), weight.ToBytes());
        }

        public RedisInt ZInterStore(string destination, int numkeys, string key, double weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<string, double>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination == null)
                throw new ArgumentNullException("destination");

            if (key == null)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.ToBytes();

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[1][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key.ToBytes())
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
                        parameters = parameters.Join(RedisCommands.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommands.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommands.Max);
                        break;
                }

                return ExpectInteger(RedisCommands.ZInterStore, parameters);
            }

            return ExpectInteger(RedisCommands.ZInterStore, destinationBytes, numkeys.ToBytes(), key.ToBytes(), weight.ToBytes());
        }

        public RedisInt ZLexCount(string key, int min, int max)
        {
            return ZLexCount(key, (double)min, (double)max);
        }

        public RedisInt ZLexCount(string key, long min, long max)
        {
            return ZLexCount(key, (double)min, (double)max);
        }

        public RedisInt ZLexCount(string key, double min, double max)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return ExpectInteger(RedisCommands.ZLexCount, key.ToBytes(), min.ToBytes(), max.ToBytes());
        }

        public RedisMultiBytes ZRange(string key, double start, double stop)
        {
            return ZRange(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRange(string key, int start, int stop)
        {
            return ZRange(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRange(string key, long start, long stop)
        {
            return ZRange(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRange(string key, string start, string stop)
        {
            if (key == null) throw new ArgumentNullException("key"); throw new NotImplementedException();
        }

        public RedisMultiBytes ZRange(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            if (stop == null)
                return ExpectMultiDataBytes(RedisCommands.ZRange, key.ToBytes(), start);

            return ExpectMultiDataBytes(RedisCommands.ZRange, key.ToBytes(), start, stop);
        }

        public RedisMultiBytes ZRangeByLex(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRangeByLex(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByLex(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRangeByLex(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByLex(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRangeByLex(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByLex(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRangeByLex(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByLex(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataBytes(RedisCommands.ZRangeByLex, parameters);
        }

        public RedisMultiString ZRangeByLexString(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRangeByLexString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByLexString(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRangeByLexString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByLexString(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRangeByLexString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByLexString(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRangeByLexString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByLexString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataStrings(RedisCommands.ZRangeByLex, parameters);
        }

        public RedisMultiBytes ZRangeByScore(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByScore(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByScore(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByScore(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRangeByScore(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataBytes(RedisCommands.ZRangeByScore, parameters);
        }

        public RedisMultiString ZRangeByScoreString(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByScoreString(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByScoreString(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByScoreString(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRangeByScoreString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataStrings(RedisCommands.ZRangeByScore, parameters);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeByScoreWithScores(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            parameters = parameters.Join(RedisCommands.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommands.ZRangeByScore, parameters);
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
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<byte[], double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeByScoreWithScoresString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            parameters = parameters.Join(RedisCommands.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommands.ZRangeByScore, parameters);
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
                        if (b != null && b.Length > 0)
                            k = Encoding.UTF8.GetString(b);

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisMultiString ZRangeString(string key, double start, double stop)
        {
            return ZRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRangeString(string key, int start, int stop)
        {
            return ZRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRangeString(string key, long start, long stop)
        {
            return ZRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRangeString(string key, string start, string stop)
        {
            return ZRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRangeString(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            if (stop == null)
                return ExpectMultiDataStrings(RedisCommands.ZRange, key.ToBytes(), start);
            return ExpectMultiDataStrings(RedisCommands.ZRange, key.ToBytes(), start, stop);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, double start, double stop)
        {
            return ZRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, int start, int stop)
        {
            return ZRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, long start, long stop)
        {
            return ZRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, string start, string stop)
        {
            return ZRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRangeWithScores(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            parameters = parameters.Join(RedisCommands.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommands.ZRange, parameters);
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
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<byte[], double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, double start, double stop)
        {
            return ZRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, int start, int stop)
        {
            return ZRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, long start, long stop)
        {
            return ZRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, string start, string stop)
        {
            return ZRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRangeWithScoresString(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            parameters = parameters.Join(RedisCommands.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommands.ZRange, parameters);
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
                        if (b != null && b.Length > 0)
                            k = Encoding.UTF8.GetString(b);

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisNullableInt ZRank(string key, string member)
        {
            return ZRank(key, member.ToBytes());
        }

        public RedisNullableInt ZRank(string key, byte[] member)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            return ExpectNullableInteger(RedisCommands.ZRank, key.ToBytes(), member);
        }

        public RedisInt ZRem(string key, string member, params string[] members)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            var length = members.Length;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(member.ToBytes()).Join(members);

                return ExpectInteger(RedisCommands.ZRem, parameters);
            }

            return ExpectInteger(RedisCommands.ZRem, key.ToBytes(), member.ToBytes());
        }

        public RedisInt ZRem(string key, byte[] member, params byte[][] members)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            var length = members.Length;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(member).Join(members);

                return ExpectInteger(RedisCommands.ZRem, parameters);
            }

            return ExpectInteger(RedisCommands.ZRem, key.ToBytes(), member);
        }

        public RedisInt ZRemRangeByLex(string key, double min, double max)
        {
            return ZRemRangeByLex(key, min.ToBytes(), max.ToBytes());
        }

        public RedisInt ZRemRangeByLex(string key, int min, int max)
        {
            return ZRemRangeByLex(key, min.ToBytes(), max.ToBytes());
        }

        public RedisInt ZRemRangeByLex(string key, long min, long max)
        {
            return ZRemRangeByLex(key, min.ToBytes(), max.ToBytes());
        }

        public RedisInt ZRemRangeByLex(string key, string min, string max)
        {
            return ZRemRangeByLex(key, min.ToBytes(), max.ToBytes());
        }

        public RedisInt ZRemRangeByLex(string key, byte[] min, byte[] max)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (min == null)
                throw new ArgumentNullException("min");

            ValidateNotDisposed();

            var length = max != null ? max.Length : 0;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(min).Join(max);

                return ExpectInteger(RedisCommands.ZRemRangeByLex, parameters);
            }

            return ExpectInteger(RedisCommands.ZRemRangeByLex, key.ToBytes(), min);
        }

        public RedisInt ZRemRangeByRank(string key, double start, double stop)
        {
            return ZRemRangeByRank(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisInt ZRemRangeByRank(string key, int start, int stop)
        {
            return ZRemRangeByRank(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisInt ZRemRangeByRank(string key, long start, long stop)
        {
            return ZRemRangeByRank(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisInt ZRemRangeByRank(string key, string start, string stop)
        {
            return ZRemRangeByRank(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisInt ZRemRangeByRank(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var length = stop != null ? stop.Length : 0;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(start).Join(stop);

                return ExpectInteger(RedisCommands.ZRemRangeByRank, parameters);
            }

            return ExpectInteger(RedisCommands.ZRemRangeByRank, key.ToBytes(), start);
        }

        public RedisInt ZRemRangeByScore(string key, double min, double max)
        {
            return ZRemRangeByScore(key, min.ToBytes(), min.ToBytes());
        }

        public RedisInt ZRemRangeByScore(string key, int min, int max)
        {
            return ZRemRangeByScore(key, min.ToBytes(), min.ToBytes());
        }

        public RedisInt ZRemRangeByScore(string key, long min, long max)
        {
            return ZRemRangeByScore(key, min.ToBytes(), min.ToBytes());
        }

        public RedisInt ZRemRangeByScore(string key, string min, string max)
        {
            return ZRemRangeByScore(key, min.ToBytes(), min.ToBytes());
        }

        public RedisInt ZRemRangeByScore(string key, byte[] min, byte[] max)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (min == null)
                throw new ArgumentNullException("min");

            ValidateNotDisposed();

            var length = max != null ? max.Length : 0;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(min).Join(max);

                return ExpectInteger(RedisCommands.ZRemRangeByScore, parameters);
            }

            return ExpectInteger(RedisCommands.ZRemRangeByScore, key.ToBytes(), min);
        }

        public RedisMultiBytes ZRevRange(string key, double start, double stop)
        {
            return ZRevRange(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRevRange(string key, int start, int stop)
        {
            return ZRevRange(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRevRange(string key, long start, long stop)
        {
            return ZRevRange(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRevRange(string key, string start, string stop)
        {
            return ZRevRange(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiBytes ZRevRange(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var length = stop != null ? stop.Length : 0;
            if (length > 0)
            {
                var parameters = key.ToBytes().Join(start).Join(stop);

                return ExpectMultiDataBytes(RedisCommands.ZRevRange, parameters);
            }

            return ExpectMultiDataBytes(RedisCommands.ZRevRange, key.ToBytes(), start);
        }

        public RedisMultiBytes ZRevRangeByScore(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRevRangeByScore(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRevRangeByScore(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRevRangeByScore(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScore(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiBytes ZRevRangeByScore(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataBytes(RedisCommands.ZRevRangeByScore, parameters);
        }

        public RedisMultiString ZRevRangeByScoreString(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRevRangeByScoreString(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRevRangeByScoreString(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRevRangeByScoreString(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisMultiString ZRevRangeByScoreString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            return ExpectMultiDataStrings(RedisCommands.ZRevRangeByScore, parameters);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScores(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeByScoreWithScores(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            parameters = parameters.Join(RedisCommands.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommands.ZRevRangeByScore, parameters);
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
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<byte[], double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, double start, double stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, int start, int stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, long start, long stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, string start, string stop, int? offset = null, int? count = null)
        {
            return ZRevRangeByScoreWithScoresString(key, start.ToBytes(), stop.ToBytes(), offset, count);
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeByScoreWithScoresString(string key, byte[] start, byte[] stop, int? offset = null, int? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            if (offset != null && count != null)
            {
                parameters = parameters.Join(RedisCommands.Limit)
                                       .Join(offset.Value.ToBytes())
                                       .Join(count.Value.ToBytes());
            }

            parameters = parameters.Join(RedisCommands.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommands.ZRevRangeByScore, parameters);
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
                        if (b != null && b.Length > 0)
                            k = Encoding.UTF8.GetString(b);

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisMultiString ZRevRangeString(string key, double start, double stop)
        {
            return ZRevRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRevRangeString(string key, int start, int stop)
        {
            return ZRevRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRevRangeString(string key, long start, long stop)
        {
            return ZRevRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRevRangeString(string key, string start, string stop)
        {
            return ZRevRangeString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisMultiString ZRevRangeString(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            return ExpectMultiDataStrings(RedisCommands.ZRevRange, parameters);
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, double start, double stop)
        {
            return ZRevRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, int start, int stop)
        {
            return ZRevRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, long start, long stop)
        {
            return ZRevRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, string start, string stop)
        {
            return ZRevRangeWithScores(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<byte[], double>[]> ZRevRangeWithScores(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            parameters = parameters.Join(RedisCommands.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommands.ZRevRange, parameters);
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
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<byte[], double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, double start, double stop)
        {
            return ZRevRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, int start, int stop)
        {
            return ZRevRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, long start, long stop)
        {
            return ZRevRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, string start, string stop)
        {
            return ZRevRangeWithScoresString(key, start.ToBytes(), stop.ToBytes());
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZRevRangeWithScoresString(string key, byte[] start, byte[] stop)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (start == null)
                throw new ArgumentNullException("start");

            ValidateNotDisposed();

            var parameters = key.ToBytes().Join(start);
            if (stop != null)
                parameters = parameters.Join(stop);

            parameters = parameters.Join(RedisCommands.WithScores);

            var bytes = ExpectMultiDataBytes(RedisCommands.ZRevRange, parameters);
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
                        if (b != null && b.Length > 0)
                            k = Encoding.UTF8.GetString(b);

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisNullableInt ZRevRank(string key, string member)
        {
            return ZRevRank(key, member.ToBytes());
        }

        public RedisNullableInt ZRevRank(string key, byte[] member)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            return ExpectNullableInteger(RedisCommands.ZRevRank, key.ToBytes(), member);
        }

        public RedisMultiBytes ZScan(string key, int cursor, string matchPattern = null, long? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (!String.IsNullOrEmpty(matchPattern) || count != null)
            {
                var parameters = key.ToBytes().Join(cursor.ToBytes());

                if (!String.IsNullOrEmpty(matchPattern))
                    parameters = parameters.Join(RedisCommands.Match).Join(matchPattern.ToBytes());

                if (count != null)
                    parameters = parameters.Join(RedisCommands.Count).Join(count.ToBytes());

                return ExpectMultiDataBytes(RedisCommands.ZScan, parameters);
            }

            return ExpectMultiDataBytes(RedisCommands.ZScan, key.ToBytes(), cursor.ToBytes());
        }

        public RedisKeyValue<byte[], byte[]>[] ZScanKeyValue(string key, int cursor, string matchPattern = null, long? count = null)
        {
            var bytes = ZScan(key, cursor, matchPattern, count);
            if (bytes != null)
            {
                var bLength = bytes.Length;
                if (bLength > 0)
                {
                    var c = bLength / 2;
                    if (bLength % 2 != 0)
                        c++;

                    var result = new RedisKeyValue<byte[], byte[]>[c];
                    for (int i = 0, index = 0; i < c; i++, index += 2)
                    {
                        var k = bytes[index];

                        var v = (byte[])null;
                        if (index < bLength - 1)
                            v = bytes[index + 1];

                        result[i] = new RedisKeyValue<byte[], byte[]>(k, v);
                    }
                }
            }
            return null;
        }

        public RedisResult<RedisKeyValue<string, double>[]> ZScanKeyValueString(string key, int cursor, string matchPattern = null, long? count = null)
        {
            var bytes = ZScan(key, cursor, matchPattern, count);
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
                        if (b != null && b.Length > 0)
                            k = Encoding.UTF8.GetString(b);

                        if (index < bLength - 1)
                        {
                            b = bytes[index + 1];
                            if (b != null && b.Length > 0)
                                double.TryParse(Encoding.UTF8.GetString(b), out d);
                        }

                        result[i] = new RedisKeyValue<string, double>(k, d);
                    }
                }
            }
            return null;
        }

        public RedisMultiString ZScanString(string key, int cursor, string matchPattern = null, long? count = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            ValidateNotDisposed();

            if (!String.IsNullOrEmpty(matchPattern) || count != null)
            {
                var parameters = key.ToBytes().Join(cursor.ToBytes());

                if (!String.IsNullOrEmpty(matchPattern))
                    parameters = parameters.Join(RedisCommands.Match).Join(matchPattern.ToBytes());

                if (count != null)
                    parameters = parameters.Join(RedisCommands.Count).Join(count.ToBytes());

                return ExpectMultiDataStrings(RedisCommands.ZScan, parameters);
            }

            return ExpectMultiDataStrings(RedisCommands.ZScan, key.ToBytes(), cursor.ToBytes());
        }

        public RedisDouble ZScore(string key, string member)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (member == null)
                throw new ArgumentNullException("member");

            ValidateNotDisposed();

            return ExpectDouble(RedisCommands.ZScore, key.ToBytes(), member.ToBytes());
        }

        public RedisInt ZUnionStore(string destination, int numkeys, string key, int weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<string, int>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination == null)
                throw new ArgumentNullException("destination");

            if (key == null)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.ToBytes();

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[1][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key.ToBytes())
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
                        parameters = parameters.Join(RedisCommands.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommands.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommands.Max);
                        break;
                }

                return ExpectInteger(RedisCommands.ZUnionStore, parameters);
            }

            return ExpectInteger(RedisCommands.ZUnionStore, destinationBytes, numkeys.ToBytes(), key.ToBytes(), weight.ToBytes());
        }

        public RedisInt ZUnionStore(string destination, int numkeys, string key, long weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<string, long>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination == null)
                throw new ArgumentNullException("destination");

            if (key == null)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.ToBytes();

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[1][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key.ToBytes())
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
                        parameters = parameters.Join(RedisCommands.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommands.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommands.Max);
                        break;
                }

                return ExpectInteger(RedisCommands.ZUnionStore, parameters);
            }

            return ExpectInteger(RedisCommands.ZUnionStore, destinationBytes, numkeys.ToBytes(), key.ToBytes(), weight.ToBytes());
        }

        public RedisInt ZUnionStore(string destination, int numkeys, string key, double weight,
                                RedisAggregate aggregate = RedisAggregate.Default,
                                params RedisKeyValue<string, double>[] keysAndWeight)
        {
            ValidateNotDisposed();

            if (destination == null)
                throw new ArgumentNullException("destination");

            if (key == null)
                throw new ArgumentNullException("key");

            var destinationBytes = destination.ToBytes();

            var kawsLength = keysAndWeight.Length;

            if (aggregate != RedisAggregate.Default || kawsLength > 0)
            {
                var parameters = new byte[1][] { destinationBytes }
                    .Join(numkeys.ToBytes())
                    .Join(key.ToBytes())
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
                        parameters = parameters.Join(RedisCommands.Sum);
                        break;
                    case RedisAggregate.Min:
                        parameters = parameters.Join(RedisCommands.Min);
                        break;
                    case RedisAggregate.Max:
                        parameters = parameters.Join(RedisCommands.Max);
                        break;
                }

                return ExpectInteger(RedisCommands.ZUnionStore, parameters);
            }

            return ExpectInteger(RedisCommands.ZUnionStore, destinationBytes, numkeys.ToBytes(), key.ToBytes(), weight.ToBytes());
        }

        #endregion Methods
    }
}
