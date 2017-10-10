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

using System.Collections.Generic;

namespace Sweet.Redis
{
    public class RedisMasterRoleInfo : RedisRoleInfo
    {
        #region .Ctors

        internal RedisMasterRoleInfo(string role)
            : base(role)
        { }

        #endregion .Ctors

        #region Properties

        public long? ReplicationOffset { get; internal set; }

        public RedisMasterRoleSlaveInfo[] Slaves { get; private set; }

        #endregion Properties

        #region Methods

        protected override void ParseInfo(RedisRawObject rawObject)
        {
            if (!ReferenceEquals(rawObject, null) && rawObject.Type == RedisRawObjectType.Array)
            {
                var list = rawObject.Items;
                if (list != null)
                {
                    var count = list.Count;
                    if (count > 1)
                    {
                        var item = list[1];
                        if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.BulkString)
                        {
                            var data = item.Data;
                            if (data is long)
                                ReplicationOffset = (long)data;
                            else if (data is int)
                                ReplicationOffset = (int)data;
                        }

                        if (count > 2)
                        {
                            var slaves = new List<RedisMasterRoleSlaveInfo>(count - 2);
                            for (var i = 2; i < count; i++)
                            {
                                var slaveInfo = RedisMasterRoleSlaveInfo.Parse(list[i]);
                                if (slaveInfo != null)
                                    slaves.Add(slaveInfo);
                            }

                            if (slaves.Count > 0)
                                Slaves = slaves.ToArray();
                        }
                    }
                }
            }
        }

        #endregion Methods
    }
}
