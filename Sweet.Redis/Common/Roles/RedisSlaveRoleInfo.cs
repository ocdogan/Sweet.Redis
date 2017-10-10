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

namespace Sweet.Redis
{
    public class RedisSlaveRoleInfo : RedisRoleInfo
    {
        #region .Ctors

        internal RedisSlaveRoleInfo(string role)
            : base(role)
        { }

        #endregion .Ctors

        #region Properties

        public string IPAddress { get; protected set; }

        public int? Port { get; protected set; }

        public string State { get; protected set; }

        public long? DataReceived { get; protected set; }

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
                            IPAddress = item.Data as string;

                        if (count > 2)
                        {
                            item = list[2];
                            if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.BulkString)
                            {
                                var data = item.Data;
                                if (data is long)
                                    Port = (int)(long)data;
                                else if (data is int)
                                    Port = (int)data;
                            }

                            if (count > 3)
                            {
                                item = list[3];
                                if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.BulkString)
                                    State = item.Data as string;

                                if (count > 4)
                                {
                                    item = list[4];
                                    if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.BulkString)
                                    {
                                        var data = item.Data;
                                        if (data is long)
                                            DataReceived = (long)data;
                                        else if (data is int)
                                            DataReceived = (int)data;
                                    }
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
