﻿#region License
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
using System.Text;

namespace Sweet.Redis
{
    /*
    Entry is composed of four (or six starting with Redis 4.0) fields:
    1) A unique progressive identifier for every slow log entry.
    2) The unix timestamp at which the logged command was processed.
    3) The amount of time needed for its execution, in microseconds.
    4) The array composing the arguments of the command.
    5) Client IP address and port (4.0 only).
    6) Client name if set via the CLIENT SETNAME command (4.0 only).
    */
    public class RedisSlowLogInfo
    {
        #region .Ctors

        public RedisSlowLogInfo(long id, DateTime time, TimeSpan duration, string clientInfo,
            string clientName, params string[] commandInfo)
        {
            Id = id;
            Time = time;
            Duration = duration;
            ClientInfo = clientInfo;
            ClientName = clientName;
            CommandInfo = commandInfo;
        }

        #endregion .Ctors

        #region Properties

        public long Id { get; private set; }

        public DateTime Time { get; private set; }

        public TimeSpan Duration { get; private set; }

        public string ClientInfo { get; private set; }

        public string ClientName { get; private set; }

        public string[] CommandInfo { get; private set; }

        #endregion Properties

        #region Methods

        #region Overrides

        public override string ToString()
        {
            var sBuilder = new StringBuilder();
            sBuilder.AppendFormat("[Id={0}, Time={1}, Duration={2}",
                Id, Time, Duration);

            if (!ClientInfo.IsEmpty())
                sBuilder.AppendFormat(", ClientInfo={0}", ClientInfo);

            if (!ClientName.IsEmpty())
                sBuilder.AppendFormat(", ClientName={0}", ClientName);

            sBuilder.Append(']');

            var cmdInfo = CommandInfo;
            if (cmdInfo != null)
            {
                var length = cmdInfo.Length;
                if (length > 0)
                {
                    sBuilder.AppendLine();
                    sBuilder.Append("CommandInfo=");
                    for (var i = 0; i < length; i++)
                    {
                        var item = cmdInfo[i];
                        if (item != null)
                        {
                            sBuilder.Append(item);
                            sBuilder.Append(' ');
                        }
                    }
                }
            }

            return sBuilder.ToString();
        }

        #endregion Overrides

        #region Static Methods

        public static RedisSlowLogInfo[] ToSlowLogInfo(RedisRawObject response)
        {
            if (response != null &&
                response.Type == RedisRawObjectType.Array)
            {
                var children = response.Items;
                if (children != null && children.Count > 0)
                {
                    var result = new List<RedisSlowLogInfo>(children.Count);

                    foreach (var child in children)
                    {
                        if (child != null &&
                            child.Type == RedisRawObjectType.Array)
                        {
                            var items = child.Items;
                            if (items != null && items.Count >= 4)
                            {
                                long id;
                                if (TryParseInteger(items[0], out id))
                                {
                                    long timeL;
                                    if (TryParseInteger(items[1], out timeL))
                                    {
                                        var time = timeL.FromUnixTimeStamp();

                                        long durationL;
                                        if (TryParseInteger(items[2], out durationL))
                                        {
                                            var duration = TimeSpan.FromMilliseconds(durationL);

                                            var commandInfo = ParseCommandInfo(items[3]);

                                            string clientInfo = null;
                                            string clientName = null;

                                            if (items.Count > 4)
                                            {
                                                clientInfo = ParseBulkString(items[4]);
                                                if (items.Count > 5)
                                                    clientName = ParseBulkString(items[5]);
                                            }

                                            result.Add(new RedisSlowLogInfo(id, time, duration, clientInfo, clientName, commandInfo));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return result.ToArray();
                }
            }
            return null;
        }

        public static RedisSlowLogInfo[] ToSlowLogInfo(IRedisRawResponse response)
        {
            if (response != null &&
                response.Type == RedisRawObjectType.Array)
            {
                var children = response.Items;
                if (children != null && children.Count > 0)
                {
                    var result = new List<RedisSlowLogInfo>(children.Count);

                    foreach (var child in children)
                    {
                        if (child != null &&
                            child.Type == RedisRawObjectType.Array)
                        {
                            var items = child.Items;
                            if (items != null && items.Count >= 4)
                            {
                                long id;
                                if (TryParseInteger(items[0], out id))
                                {
                                    long timeL;
                                    if (TryParseInteger(items[1], out timeL))
                                    {
                                        var time = timeL.FromUnixTimeStamp();

                                        long durationL;
                                        if (TryParseInteger(items[2], out durationL))
                                        {
                                            var duration = TimeSpan.FromMilliseconds(durationL);

                                            var commandInfo = ParseCommandInfo(items[3]);

                                            string clientInfo = null;
                                            string clientName = null;

                                            if (items.Count > 4)
                                            {
                                                clientInfo = ParseBulkString(items[4]);
                                                if (items.Count > 5)
                                                    clientName = ParseBulkString(items[5]);
                                            }

                                            result.Add(new RedisSlowLogInfo(id, time, duration, clientInfo, clientName, commandInfo));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return result.ToArray();
                }
            }
            return null;
        }

        private static bool TryParseInteger(RedisRawObject response, out long value)
        {
            value = RedisConstants.Zero;

            if (response != null &&
                response.Type == RedisRawObjectType.Integer)
            {
                var data = response.Data;
                if (data != null && data is long)
                {
                    value = (long)data;
                    return true;
                }
            }
            return false;
        }

        private static bool TryParseInteger(IRedisRawResponse response, out long value)
        {
            value = RedisConstants.Zero;

            if (response != null &&
                response.Type == RedisRawObjectType.Integer)
            {
                var data = response.Data;
                if (!data.IsEmpty())
                {
                    value = RedisConstants.MinusOne;
                    if (long.TryParse(data.ToUTF8String(), out value))
                        return true;
                }
            }
            return false;
        }

        private static string[] ParseCommandInfo(IRedisRawResponse response)
        {
            if (response != null &&
                response.Type == RedisRawObjectType.Array)
            {
                var items = response.Items;
                if (items != null)
                {
                    var count = items.Count;
                    if (count > 0)
                    {
                        var result = new List<string>(count);

                        foreach (var item in items)
                        {
                            if (item != null &&
                                item.Type == RedisRawObjectType.BulkString)
                            {
                                var data = item.Data;
                                if (data == null)
                                    result.Add(null);
                                else if (data.Length == 0)
                                    result.Add(String.Empty);
                                else
                                    result.Add(data.ToUTF8String());
                            }
                        }

                        return result.ToArray();
                    }
                }
            }
            return null;
        }

        private static string[] ParseCommandInfo(RedisRawObject response)
        {
            if (response != null &&
                response.Type == RedisRawObjectType.Array)
            {
                var items = response.Items;
                if (items != null)
                {
                    var count = items.Count;
                    if (count > 0)
                    {
                        var result = new List<string>(count);

                        foreach (var item in items)
                        {
                            if (item != null &&
                                item.Type == RedisRawObjectType.BulkString)
                                result.Add(item.DataText);
                        }

                        return result.ToArray();
                    }
                }
            }
            return null;
        }

        private static string ParseBulkString(IRedisRawResponse response)
        {
            if (response != null &&
                response.Type == RedisRawObjectType.BulkString)
            {
                var data = response.Data;
                if (data == null)
                    return null;

                if (data.Length == 0)
                    return String.Empty;

                return data.ToUTF8String();
            }
            return null;
        }

        private static string ParseBulkString(RedisRawObject response)
        {
            if (response != null &&
                response.Type == RedisRawObjectType.BulkString)
                return response.DataText;
            return null;
        }

        #endregion Static Methods

        #endregion Methods
    }
}
