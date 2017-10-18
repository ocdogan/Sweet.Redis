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
    public class RedisRoleInfo
    {
        #region .Ctors

        internal RedisRoleInfo(string role)
        {
            RoleName = (role ?? String.Empty).ToLowerInvariant();
            switch (role)
            {
                case "master":
                    Role = RedisRole.Master;
                    break;
                case "slave":
                    Role = RedisRole.Slave;
                    break;
                case "sentinel":
                    Role = RedisRole.Sentinel;
                    break;
                default:
                    Role = RedisRole.Undefined;
                    break;
            }
        }

        #endregion .Ctors

        #region Properties

        public RedisRole Role { get; private set; }

        public string RoleName { get; private set; }

        #endregion Properties

        #region Methods

        protected virtual void ParseInfo(RedisRawObject rawObject)
        { }

        public static RedisRoleInfo Parse(RedisRawObject rawObject)
        {
            if (!ReferenceEquals(rawObject, null) && rawObject.Type == RedisRawObjectType.Array)
            {
                var list = rawObject.Items;
                if (list != null)
                {
                    var count = list.Count;
                    if (count > 0)
                    {
                        var item = list[0];
                        if (!ReferenceEquals(item, null) && item.Type == RedisRawObjectType.BulkString)
                        {
                            var data = item.Data;
                            if (data != null)
                            {
                                var role = item.DataText;
                                if (!String.IsNullOrEmpty(role))
                                {
                                    role = role.ToLowerInvariant();
                                    switch (role)
                                    {
                                        case "master":
                                            {
                                                var result = new RedisMasterRoleInfo(role);
                                                result.ParseInfo(rawObject);
                                                return result;
                                            }
                                        case "slave":
                                            {
                                                var result = new RedisSlaveRoleInfo(role);
                                                result.ParseInfo(rawObject);
                                                return result;
                                            }
                                        case "sentinel":
                                            {
                                                var result = new RedisSentinelRoleInfo(role);
                                                result.ParseInfo(rawObject);
                                                return result;
                                            }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return null;
        }

        #endregion Methods
    }
}
