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
using System.Net.Sockets;

namespace Sweet.Redis
{
    internal class RedisSentinelConnection : RedisBidirectionalConnection
    {
        #region .Ctors

        internal RedisSentinelConnection(string name, RedisSentinelSettings settings,
            Action<RedisConnection, RedisSocket> onCreateSocket, Action<RedisConnection, RedisSocket> onReleaseSocket,
            RedisSocket socket = null, bool connectImmediately = false)
            : base(name, RedisRole.Sentinel, settings, onCreateSocket, onReleaseSocket, socket, connectImmediately)
        { }

        #endregion .Ctors

        #region Member Methods

        protected override RedisRole DiscoverRole(RedisSocket socket)
        {
            var role = base.DiscoverRole(socket);
            ValidateRole(RedisRole.Sentinel);
            return role;
        }

        #endregion Member Methods
    }
}
