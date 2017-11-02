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
    master0:name=mymaster,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=1
    */
    public class RedisServerMasterInfo : RedisInfoBase
    {
        #region .Ctors

        internal RedisServerMasterInfo(int index, string masterInfo)
        {
            Index = index;
            Parse(masterInfo);
        }

        #endregion .Ctors

        #region Properties

        public int Index { get; private set; } // 0

        public string IPAddress { get; private set; } // 127.0.0.1

        public string Name { get; private set; } // mymaster

        public int? Port { get; private set; } // 1

        public string Status { get; private set; } // ok

        public int? Slaves { get; private set; } // 2

        public int? Sentinels { get; private set; } // 1

        #endregion Properties

        #region Methods

        private void Parse(string masterInfo)
        {
            if (!masterInfo.IsEmpty())
            {
                var parts = masterInfo.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts != null)
                {
                    var length = parts.Length;
                    if (length > 0)
                    {
                        for (var i = 0; i < length; i++)
                        {
                            var part = parts[i].Trim();
                            if (!part.IsEmpty())
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

                                if (!name.IsEmpty())
                                {
                                    switch (name)
                                    {
                                        case "address":
                                            {
                                                var str = (value ?? String.Empty).Trim();
                                                if (!str.IsEmpty())
                                                {
                                                    pos = str.IndexOf(':');
                                                    if (pos == -1)
                                                        IPAddress = str;
                                                    else if (pos == str.Length - 1)
                                                        IPAddress = str.Substring(0, str.Length - 1);
                                                    else
                                                    {
                                                        IPAddress = str.Substring(0, pos);

                                                        int port;
                                                        if (int.TryParse(str.Substring(pos + 1, str.Length - pos - 1), out port))
                                                            Port = port;
                                                    }
                                                }
                                            }
                                            break;
                                        case "name":
                                            Name = value;
                                            break;
                                        case "status":
                                            Status = value;
                                            break;
                                        case "slaves":
                                            {
                                                int slaves;
                                                if (int.TryParse(value, out slaves))
                                                    Slaves = slaves;
                                            }
                                            break;
                                        case "sentinels":
                                            {
                                                int sentinels;
                                                if (int.TryParse(value, out sentinels))
                                                    Sentinels = sentinels;
                                            }
                                            break;
                                        default:
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
