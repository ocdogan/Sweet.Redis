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
    internal class RedisBidirectionalConnection : RedisConnection
    {
        #region .Ctors

        internal RedisBidirectionalConnection(string name, RedisRole expectedRole, RedisConnectionSettings settings,
            Action<RedisConnection, RedisSocket> onCreateSocket, Action<RedisConnection, RedisSocket> onReleaseSocket,
            RedisSocket socket = null, bool connectImmediately = false)
            : base(name, expectedRole, settings, onCreateSocket, onReleaseSocket, socket, connectImmediately)
        { }

        #endregion .Ctors

        #region Methods

        public override RedisRawResponse SendReceive(byte[] data, RedisRole commandRole)
        {
            ValidateNotDisposed();
            ValidateRole(commandRole);

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new RedisFatalException(new SocketException((int)SocketError.NotConnected), RedisErrorCode.ConnectionError);
            }

            var task = socket.SendAsync(data, 0, data.Length)
                .ContinueWith<RedisRawResponse>((asyncTask) =>
                {
                    if (asyncTask.IsFaulted && asyncTask.Exception.IsSocketError())
                    {
                        FreeAndNilSocket();
                        return null;
                    }

                    if (asyncTask.IsCompleted && asyncTask.Result > 0)
                        using (var reader = new RedisSingleResponseReader(Settings))
                            return reader.Execute(socket);
                    return null;
                });
            return task.Result;
        }

        public override RedisRawResponse SendReceive(IRedisCommand cmd)
        {
            if (cmd == null)
                throw new RedisFatalException(new ArgumentNullException("cmd"), RedisErrorCode.MissingParameter);

            ValidateNotDisposed();
            ValidateRole(cmd.Role);

            var socket = Connect();
            if (socket == null)
            {
                SetLastError((long)SocketError.NotConnected);
                SetState((long)RedisConnectionState.Failed);

                throw new SocketException((int)SocketError.NotConnected);
            }

            try
            {
                cmd.WriteTo(socket);
                using (var reader = new RedisSingleResponseReader(Settings))
                    return reader.Execute(socket);
            }
            catch (Exception e)
            {
                if (e.IsSocketError())
                    FreeAndNilSocket();
                throw;
            }
        }

        #endregion Methods
    }
}