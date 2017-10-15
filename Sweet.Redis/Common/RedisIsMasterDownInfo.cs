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
    public struct RedisIsMasterDownInfo
    {
        #region Static Members

        public static readonly RedisIsMasterDownInfo Empty = new RedisIsMasterDownInfo();

        #endregion Static Members

        #region .Ctors

        public RedisIsMasterDownInfo(bool isDown, string leaderRunId, long leaderEpoch)
            : this()
        {
            IsDown = isDown;
            LeaderRunId = leaderRunId;
            LeaderEpoch = leaderEpoch;
        }

        #endregion .Ctors

        #region Properties

        public bool? IsDown { get; private set; }

        public string LeaderRunId { get; private set; }

        public long LeaderEpoch { get; private set; }

        #endregion Public Fields

        #region Properties

        public static RedisIsMasterDownInfo Parse(RedisRawObject rawObject)
        {
            var result = new RedisIsMasterDownInfo();
            if (!ReferenceEquals(rawObject, null) && rawObject.Type == RedisRawObjectType.Array)
            {
                var items = rawObject.Items;
                if (items != null)
                {
                    var count = items.Count;
                    if (count > 0)
                    {
                        var item = items[0];
                        if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Integer)
                        {
                            var data = item.Data;
                            if (data is long)
                                result.IsDown = (long)data == RedisConstants.One;
                            else if (data is int)
                                result.IsDown = (int)data == RedisConstants.One;

                            if (count > 1)
                            {
                                item = items[1];
                                if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.BulkString)
                                {
                                    result.LeaderRunId = item.DataText ?? "*";

                                    if (count > 2)
                                    {
                                        item = items[2];
                                        if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Integer)
                                        {
                                            data = item.Data;
                                            if (data is long)
                                                result.LeaderEpoch = (long)data;
                                            else if (data is int)
                                                result.LeaderEpoch = (int)data;
                                        }
                                    }
                                }
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
