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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;


namespace Sweet.Redis
{
    internal class RedisSingleResponseReader : RedisResponseReader
    {
        #region .Ctors

        public RedisSingleResponseReader()
            : base(16 * 1024)
        { }

        #endregion .Ctors

        #region Methods

        public IRedisResponse Execute(RedisSocket socket)
        {
            if (socket.IsConnected() &&
                Interlocked.CompareExchange(ref m_ReadState, RedisConstants.One, RedisConstants.Zero) == RedisConstants.Zero)
            {
                try
                {
                    var result = base.ReadResponse(socket);
                    if (result != null && result.IsVoid)
                        return null;
                    return result;
                }
                finally
                {
                    Interlocked.Exchange(ref m_ReadState, RedisConstants.Zero);
                }
            }
            return null;
        }

        #endregion Methods
    }
}
