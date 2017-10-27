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
    public struct RedisMonitorMessage
    {
        #region Static Members

        public static readonly RedisMonitorMessage Empty = new RedisMonitorMessage(-1);

        #endregion Static Members

        #region .Ctors

        public RedisMonitorMessage(int dummy = -1)
            : this()
        {
            ClientInfo = "";
            Data = null;
            Time = DateTime.MinValue;
            IsEmpty = true;
        }

        public RedisMonitorMessage(DateTime time, string clientInfo, byte[] data)
            : this()
        {
            ClientInfo = clientInfo;
            Data = data;
            Time = time;
        }

        #endregion .Ctors

        #region Properties

        public string ClientInfo { get; private set; }

        public byte[] Data { get; private set; }

        public bool IsEmpty { get; private set; }

        public DateTime Time { get; private set; }

        #endregion Properties

        #region Methods

        public static RedisMonitorMessage ToMonitorMessage(IRedisRawResponse response)
        {
            if (!ReferenceEquals(response, null) &&
                response.Type == RedisRawObjectType.SimpleString)
            {
                var data = response.Data;
                if (data != null)
                {
                    var timeEndPos = data.IndexOf((byte)' ', 0, 100);
                    if (timeEndPos > 0)
                    {
                        var timeStr = Encoding.UTF8.GetString(data, 0, timeEndPos);

                        if (!String.IsNullOrEmpty(timeStr))
                        {
                            var clientStartPos = data.IndexOf((byte)'[', timeEndPos + 1, 10);
                            if (clientStartPos > -1)
                            {
                                var clientEndPos = data.IndexOf((byte)']', clientStartPos + 1, 100);
                                if (clientEndPos > -1)
                                {
                                    var time = DateTime.MinValue;

                                    var dotPos = timeStr.IndexOf('.');
                                    if (dotPos < 0)
                                        time = timeStr.ToInt().FromUnixTimeStamp();
                                    else
                                    {
                                        time = ((dotPos == 0) ? "0" : timeStr.Substring(0, dotPos)).ToInt()
                                            .FromUnixTimeStamp(timeStr.Substring(dotPos + 1, timeStr.Length - dotPos - 1).ToInt(0));
                                    }

                                    byte[] msgData = null;
                                    var clientInfo = Encoding.UTF8.GetString(data, clientStartPos, clientEndPos - clientStartPos + 1);

                                    clientEndPos += 2;
                                    if (clientEndPos < data.Length)
                                    {
                                        msgData = new byte[data.Length - clientEndPos];
                                        Buffer.BlockCopy(data, clientEndPos, msgData, 0, msgData.Length);
                                    }

                                    return new RedisMonitorMessage(time, clientInfo, msgData);
                                }
                            }
                        }
                    }
                }
            }
            return RedisMonitorMessage.Empty;
        }

        #endregion Methods
    }
}
