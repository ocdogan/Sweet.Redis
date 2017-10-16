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
    /*
    slave0:ip=127.0.0.1,port=6381,state=online,offset=1748378,lag=0
    */
    public class RedisServerSlaveInfo : RedisInfoBase
    {
        #region .Ctors

        internal RedisServerSlaveInfo(int index, string slaveInfo)
        {
            Index = index;
            Parse(slaveInfo);
        }

        #endregion .Ctors

        #region Properties

        public int Index { get; private set; } // 127.0.0.1

        public string IPAddress { get; private set; } // 127.0.0.1

        public int? Port { get; private set; } // 1

        public string State { get; private set; } // online

        public long? Offset { get; private set; } // 1748378

        public long? Lag { get; private set; } // 0

        #endregion Properties

        #region Methods

        private void Parse(string slaveInfo)
        {
            if (!String.IsNullOrEmpty(slaveInfo))
            {
                var parts = slaveInfo.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts != null)
                {
                    var length = parts.Length;
                    if (length > 0)
                    {
                        for (var i = 0; i < length; i++)
                        {
                            var part = parts[i].Trim();
                            if (!String.IsNullOrEmpty(part))
                            {
                                string name, value = null;

                                var pos = part.IndexOf('=');
                                if (pos == -1)
                                    name = part;
                                else
                                {
                                    name = part.Substring(0, pos).TrimEnd();
                                    if (pos < part.Length - 1)
                                        value = part.Substring(pos + 1).TrimStart();
                                }

                                if (!String.IsNullOrEmpty(name))
                                {
                                    switch (name)
                                    {
                                        case "ip":
                                            IPAddress = value;
                                            break;
                                        case "port":
                                            {
                                                int port;
                                                if (int.TryParse(value, out port))
                                                    Port = port;
                                            }
                                            break;
                                        case "state":
                                            State = value;
                                            break;
                                        case "offset":
                                            {
                                                long offset;
                                                if (long.TryParse(value, out offset))
                                                    Offset = offset;
                                            }
                                            break;
                                        case "lag":
                                            {
                                                long lag;
                                                if (long.TryParse(value, out lag))
                                                    Lag = lag;
                                            }
                                            break;
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
