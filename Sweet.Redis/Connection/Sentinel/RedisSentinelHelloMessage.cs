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
    127.0.0.1,26380,07fabf3cbac43bcc955588b1023f95498b58f8f2,16,mymaster,127.0.0.1,6381,16
    */
    public struct RedisSentinelHelloMessage
    {
        #region .Ctors

        public RedisSentinelHelloMessage(string sentinelIP = null, int? sentinelPort = null, string sentinelId = null,
            long? sentinelCurrentEpoch = null, string masterName = null, string masterIPAddress = null, int? masterPort = null,
            long? masterConfigEpoch = null)
            : this()
        {
            SentinelIPAddress = sentinelIP;
            SentinelPort = sentinelPort;
            SentinelId = sentinelId;
            SentinelCurrentEpoch = sentinelCurrentEpoch;
            MasterName = masterName;
            MasterIPAddress = masterIPAddress;
            MasterPort = masterPort;
            MasterConfigEpoch = masterConfigEpoch;
        }

        #endregion .Ctors

        #region Properties

        public string SentinelIPAddress { get; private set; }

        public int? SentinelPort { get; private set; }

        public string SentinelId { get; private set; }

        public long? SentinelCurrentEpoch { get; private set; }

        public string MasterName { get; private set; }

        public string MasterIPAddress { get; private set; }

        public int? MasterPort { get; private set; }
        
        public long? MasterConfigEpoch { get; private set; }

        #endregion Properties

        #region Methods

        public static RedisSentinelHelloMessage Parse(string message)
        {
            if (!message.IsEmpty())
            {
                var parts = message.Split(new[] { ',' }, StringSplitOptions.None);

                var length = parts.Length;
                if (length > 0)
                {
                    /* 0 */ 
                    var sentinelIP = parts[0];
                    /* 1 */ 
                    var sentinelPort = (int?)null;
                    /* 2 */
                    var sentinelId = (string)null;
                    /* 3 */
                    var sentinelCurrentEpoch = (long?)null;
                    /* 4 */
                    var masterName = (string)null;
                    /* 5 */
                    var masterIPAddress = (string)null;
                    /* 6 */
                    var masterPort = (int?)null;
                    /* 7 */
                    var masterConfigEpoch = (long?)null;

                    if (length > 1)
                    {
                        int i;
                        if (int.TryParse(parts[1], out i))
                            sentinelPort = i;

                        if (length > 2)
                        {
                            sentinelId = parts[2];
                            if (length > 3)
                            {
                                long l;
                                if (long.TryParse(parts[1], out l))
                                    sentinelCurrentEpoch = l;

                                if (length > 4)
                                {
                                    masterName = parts[4];
                                    if (length > 5)
                                    {
                                        masterIPAddress = parts[5];
                                        if (length > 6)
                                        {
                                            if (int.TryParse(parts[1], out i))
                                                masterPort = i;

                                            if (length > 7 &&
                                                long.TryParse(parts[1], out l))
                                                masterConfigEpoch = l;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    return new RedisSentinelHelloMessage(sentinelIP, sentinelPort, sentinelId, sentinelCurrentEpoch, 
                        masterName, masterIPAddress, masterPort, masterConfigEpoch);
                }
            }
            return new RedisSentinelHelloMessage();
        }

        #endregion Methods
    }
}
