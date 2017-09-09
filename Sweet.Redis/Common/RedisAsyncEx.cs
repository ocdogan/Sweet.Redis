using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Sweet.Redis
{
    internal static class RedisAsyncEx
    {
        #region Methods

        #region Dns

        public static Task<IPAddress> GetHostAddressesAsync(string host)
        {
            var tcs = new TaskCompletionSource<IPAddress>(null);

            Dns.BeginGetHostAddresses(host, ar =>
                {
                    var innerTcs = (TaskCompletionSource<IPAddress>)ar.AsyncState;
                    try
                    {
                        var addrs = Dns.EndGetHostAddresses(ar);
                        innerTcs.TrySetResult((addrs != null && addrs.Length > 0) ? addrs[0] : null);
                    }
                    catch (Exception ex)
                    {
                        innerTcs.TrySetException(ex);
                    }
                }, tcs);
            return tcs.Task;
        }

        #endregion Dns

        #region Socket

        public static Task ConnectAsync(this Socket socket, IPEndPoint endPoint)
        {
            var tcs = new TaskCompletionSource<object>(socket);

            socket.BeginConnect(endPoint, ar =>
            {
                var innerTcs = (TaskCompletionSource<object>)ar.AsyncState;
                try
                {
                    ((Socket)innerTcs.Task.AsyncState).EndConnect(ar);
                    innerTcs.TrySetResult(null);
                }
                catch (Exception ex)
                {
                    innerTcs.TrySetException(ex);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task ConnectAsync(this Socket socket, EndPoint remoteEP)
        {
            var tcs = new TaskCompletionSource<bool>(socket);
            socket.BeginConnect(remoteEP, iar =>
            {
                var innerTcs = (TaskCompletionSource<bool>)iar.AsyncState;
                try
                {
                    ((Socket)innerTcs.Task.AsyncState).EndConnect(iar);
                    innerTcs.TrySetResult(true);
                }
                catch (Exception e)
                {
                    innerTcs.TrySetException(e);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task ConnectAsync(this Socket socket, IPAddress address, int port)
        {
            var tcs = new TaskCompletionSource<bool>(socket);
            socket.BeginConnect(address, port, iar =>
            {
                var innerTcs = (TaskCompletionSource<bool>)iar.AsyncState;
                try
                {
                    ((Socket)innerTcs.Task.AsyncState).EndConnect(iar);
                    innerTcs.TrySetResult(true);
                }
                catch (Exception e)
                {
                    innerTcs.TrySetException(e);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task ConnectAsync(this Socket socket, IPAddress[] addresses, int port)
        {
            var tcs = new TaskCompletionSource<bool>(socket);
            socket.BeginConnect(addresses, port, iar =>
            {
                var innerTcs = (TaskCompletionSource<bool>)iar.AsyncState;
                try
                {
                    ((Socket)innerTcs.Task.AsyncState).EndConnect(iar);
                    innerTcs.TrySetResult(true);
                }
                catch (Exception e)
                {
                    innerTcs.TrySetException(e);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task ConnectAsync(this Socket socket, string host, int port)
        {
            var tcs = new TaskCompletionSource<bool>(socket);
            socket.BeginConnect(host, port, iar =>
            {
                var innerTcs = (TaskCompletionSource<bool>)iar.AsyncState;
                try
                {
                    ((Socket)innerTcs.Task.AsyncState).EndConnect(iar);
                    innerTcs.TrySetResult(true);
                }
                catch (Exception e)
                {
                    innerTcs.TrySetException(e);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task DisconnectAsync(this Socket socket, bool reuseSocket = false)
        {
            var tcs = new TaskCompletionSource<object>(socket);

            socket.BeginDisconnect(reuseSocket, ar =>
            {
                var innerTcs = (TaskCompletionSource<object>)ar.AsyncState;
                try
                {
                    ((Socket)innerTcs.Task.AsyncState).EndDisconnect(ar);
                    innerTcs.TrySetResult(null);
                }
                catch (Exception ex)
                {
                    innerTcs.TrySetException(ex);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task<int> SendAsync(this Socket socket, byte[] data, int offset, int count, SocketFlags socketFlags = SocketFlags.None)
        {
            var tcs = new TaskCompletionSource<int>(socket);

            socket.BeginSend(data, offset, count, socketFlags, ar =>
            {
                var innerTcs = (TaskCompletionSource<int>)ar.AsyncState;
                try
                {
                    innerTcs.TrySetResult(((Socket)innerTcs.Task.AsyncState).EndSend(ar));
                }
                catch (Exception ex)
                {
                    innerTcs.TrySetException(ex);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task<int> ReceiveAsync(this Socket socket, byte[] data, int offset, int count, SocketFlags socketFlags = SocketFlags.None)
        {
            var tcs = new TaskCompletionSource<int>(socket);

            socket.BeginReceive(data, offset, count, socketFlags, ar =>
            {
                var innerTcs = (TaskCompletionSource<int>)ar.AsyncState;
                try
                {
                    innerTcs.TrySetResult(((Socket)innerTcs.Task.AsyncState).EndReceive(ar));
                }
                catch (Exception ex)
                {
                    innerTcs.TrySetException(ex);
                }
            }, tcs);
            return tcs.Task;
        }

        #endregion Socket

        #region Stream

        public static Task<bool> WriteAsync(this Stream stream, byte[] data, int offset, int count)
        {
            var tcs = new TaskCompletionSource<bool>(stream);

            stream.BeginWrite(data, offset, count, ar =>
            {
                var innerTcs = (TaskCompletionSource<bool>)ar.AsyncState;
                try
                {
                    ((Stream)innerTcs.Task.AsyncState).EndWrite(ar);
                    innerTcs.TrySetResult(true);
                }
                catch (Exception ex)
                {
                    innerTcs.TrySetException(ex);
                }
            }, tcs);
            return tcs.Task;
        }

        public static Task<int> ReadAsync(this Stream stream, byte[] data, int offset, int count)
        {
            var tcs = new TaskCompletionSource<int>(stream);

            stream.BeginRead(data, offset, count, ar =>
            {
                var innerTcs = (TaskCompletionSource<int>)ar.AsyncState;
                try
                {
                    innerTcs.TrySetResult(((Stream)innerTcs.Task.AsyncState).EndRead(ar));
                }
                catch (Exception ex)
                {
                    innerTcs.TrySetException(ex);
                }
            }, tcs);
            return tcs.Task;
        }

        #endregion Stream

        #endregion Methods
    }
}
