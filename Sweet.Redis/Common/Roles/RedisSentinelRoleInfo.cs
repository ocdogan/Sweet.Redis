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
    public class RedisSentinelRoleInfo : RedisRoleInfo
    {
        #region .Ctors

        internal RedisSentinelRoleInfo(string role)
            : base(role)
        { }

        #endregion .Ctors

        #region Properties

        public string[] MasterNames { get; protected set; }

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
                        if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.Array)
                        {
                            var items = item.Items;
                            if (items != null)
                            {
                                var itemCount = items.Count;
                                if (itemCount > 0)
                                {
                                    var masterNames = new List<string>(itemCount);
                                    for (var i = 0; i < itemCount; i++)
                                    {
                                        item = items[i];
                                        if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.BulkString)
                                        {
                                            var masterName = item.DataText;
                                            if (!String.IsNullOrEmpty(masterName))
                                                masterNames.Add(masterName);
                                        }
                                    }

                                    if (masterNames.Count > 0)
                                        MasterNames = masterNames.ToArray();
                                }
                            }
                        }
                    }
                }
            }
        }

        #endregion Methods
    }
}