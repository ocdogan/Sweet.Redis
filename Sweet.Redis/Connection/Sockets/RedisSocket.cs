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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisSocket : RedisDisposable
    {
        #region Field Members

        private int m_DbIndex = -1;
        private bool m_Authenticated;

        private long m_Id;

        private RedisRole m_Role = RedisRole.Undefined;

        private bool m_UseSsl;
        private readonly Socket m_Socket;

        private Action<RedisSocket> m_OnConnect;
        private Action<RedisSocket> m_OnDisconnect;

        private IPEndPoint m_RemoteEP;
        private Stream m_RealStream;
        private BufferedStream m_BufferedStream;

        private LocalCertificateSelectionCallback m_SslCertificateSelection;
        private RemoteCertificateValidationCallback m_SslCertificateValidation;

        #endregion Field Members

        #region Constructors

        private RedisSocket(Socket socket, bool useSsl = false,
                           LocalCertificateSelectionCallback sslCertificateSelection = null,
                           RemoteCertificateValidationCallback sslCertificateValidation = null,
                           Action<RedisSocket> onConnect = null, Action<RedisSocket> onDisconnect = null)
        {
            m_Id = RedisIDGenerator<RedisSocket>.NextId();
            m_Socket = socket;
            m_UseSsl = useSsl;
            m_OnConnect = onConnect;
            m_OnDisconnect = onDisconnect;
            m_SslCertificateSelection = sslCertificateSelection;
            m_SslCertificateValidation = sslCertificateValidation;
        }

        public RedisSocket(SocketInformation socketInformation, bool useSsl = false,
                           LocalCertificateSelectionCallback sslCertificateSelection = null,
                           RemoteCertificateValidationCallback sslCertificateValidation = null,
                           Action<RedisSocket> onConnect = null, Action<RedisSocket> onDisconnect = null)
        {
            m_Id = RedisIDGenerator<RedisSocket>.NextId();
            m_UseSsl = useSsl;
            m_OnConnect = onConnect;
            m_OnDisconnect = onDisconnect;
            m_SslCertificateSelection = sslCertificateSelection;
            m_SslCertificateValidation = sslCertificateValidation;
            m_Socket = new RedisNativeSocket(socketInformation);
        }

        public RedisSocket(AddressFamily addressFamily, SocketType socketType,
                           ProtocolType protocolType, bool useSsl = false,
                           LocalCertificateSelectionCallback sslCertificateSelection = null,
                           RemoteCertificateValidationCallback sslCertificateValidation = null,
                           Action<RedisSocket> onConnect = null, Action<RedisSocket> onDisconnect = null)
        {
            m_Id = RedisIDGenerator<RedisSocket>.NextId();
            m_UseSsl = useSsl;
            m_OnConnect = onConnect;
            m_OnDisconnect = onDisconnect;
            m_SslCertificateSelection = sslCertificateSelection;
            m_SslCertificateValidation = sslCertificateValidation;
            m_Socket = new RedisNativeSocket(addressFamily, socketType, protocolType);
        }

        #endregion Constructors

        #region Destructors

        protected override void OnDispose(bool disposing)
        {
            var onDisconnect = Interlocked.Exchange(ref m_OnDisconnect, null);
            var wasConnected = (onDisconnect != null) && m_Socket.IsConnected();

            Interlocked.Exchange(ref m_SslCertificateSelection, null);
            Interlocked.Exchange(ref m_SslCertificateValidation, null);

            var rs = Interlocked.Exchange(ref m_RealStream, null);
            if (rs != null)
                rs.Dispose();

            var ws = Interlocked.Exchange(ref m_BufferedStream, null);
            if (ws != null)
                ws.Dispose();

            base.OnDispose(disposing);

            if (wasConnected && (onDisconnect != null) && !m_Socket.IsConnected())
                onDisconnect(this);
        }

        #endregion Destructors

        #region Static Properties

        public static bool OSSupportsIPv6
        {
            get
            {
                return Socket.OSSupportsIPv6;
            }
        }

        public static bool OSSupportsIPv4
        {
            get
            {
                return Socket.SupportsIPv4;
            }
        }

        #endregion Static Properties

        #region Properties

        public AddressFamily AddressFamily
        {
            get
            {
                return m_Socket.AddressFamily;
            }
        }

        public bool Authenticated
        {
            get { return m_Authenticated; }
        }

        public int Available
        {
            get
            {
                return m_Socket.Available;
            }
        }

        public bool Blocking
        {
            get
            {
                return m_Socket.Blocking;
            }
            set
            {
                m_Socket.Blocking = value;
            }
        }

        public bool Connected
        {

            get
            {
                return m_Socket.Connected;
            }
        }

        public int DbIndex
        {
            get { return m_DbIndex; }
        }

        public bool DontFragment
        {

            get
            {
                return m_Socket.DontFragment;
            }
            set
            {
                m_Socket.DontFragment = value;
            }
        }

        public bool EnableBroadcast
        {

            get
            {
                return m_Socket.EnableBroadcast;
            }
            set
            {
                m_Socket.EnableBroadcast = value;
            }
        }

        public bool ExclusiveAddressUse
        {

            get
            {
                return m_Socket.ExclusiveAddressUse;
            }
            set
            {
                m_Socket.ExclusiveAddressUse = value;
            }
        }

        public IntPtr Handle
        {

            get
            {
                return m_Socket.Handle;
            }
        }

        public long Id
        {
            get
            {
                return m_Id;
            }
        }

        public bool IsBound
        {

            get
            {
                return m_Socket.IsBound;
            }
        }

        public LingerOption LingerState
        {

            get
            {
                return m_Socket.LingerState;
            }
            set
            {
                m_Socket.LingerState = value;
            }
        }

        public EndPoint LocalEndPoint
        {

            get
            {
                return m_Socket.LocalEndPoint;
            }
        }

        public bool MulticastLoopback
        {

            get
            {
                return m_Socket.MulticastLoopback;
            }
            set
            {
                m_Socket.MulticastLoopback = value;
            }
        }

        public bool NoDelay
        {

            get
            {
                return m_Socket.NoDelay;
            }
            set
            {
                m_Socket.NoDelay = value;
            }
        }

        public ProtocolType ProtocolType
        {

            get
            {
                return m_Socket.ProtocolType;
            }
        }

        public int ReceiveBufferSize
        {

            get
            {
                return m_Socket.ReceiveBufferSize;
            }
            set
            {
                m_Socket.ReceiveBufferSize = value;
            }
        }

        public int ReceiveTimeout
        {

            get
            {
                return m_Socket.ReceiveTimeout;
            }
            set
            {
                m_Socket.ReceiveTimeout = value;
            }
        }

        public IPEndPoint RemoteEP
        {
            get { return m_RemoteEP; }
        }

        public EndPoint RemoteEndPoint
        {

            get
            {
                return m_Socket.RemoteEndPoint;
            }
        }

        public RedisRole Role
        {
            get
            {
                return m_Role;
            }
            set
            {
                m_Role = value;
            }
        }

        public int SendBufferSize
        {

            get
            {
                return m_Socket.SendBufferSize;
            }
            set
            {
                m_Socket.SendBufferSize = value;
            }
        }

        public int SendTimeout
        {

            get
            {
                return m_Socket.SendTimeout;
            }
            set
            {
                m_Socket.SendTimeout = value;
            }
        }

        public SocketType SocketType
        {

            get
            {
                return m_Socket.SocketType;
            }
        }

        public short Ttl
        {

            get
            {
                return m_Socket.Ttl;
            }
            set
            {
                m_Socket.Ttl = value;
            }
        }

        public bool UseOnlyOverlappedIO
        {

            get
            {
                return m_Socket.UseOnlyOverlappedIO;
            }
            set
            {
                m_Socket.UseOnlyOverlappedIO = value;
            }
        }

        public bool UseSsl
        {
            get { return m_UseSsl; }
        }

        #endregion Properties

        #region Static Methods

        public static void Select(IList checkRead, IList checkWrite, IList checkError, int microSeconds)
        {
            Socket.Select(checkRead, checkWrite, checkError, microSeconds);
        }

        #endregion Static Methods

        #region Methods

        #region Redis Based Methods

        internal void SetAuthenticated(bool value)
        {
            m_Authenticated = value;
        }

        internal void SetDb(int dbIndex)
        {
            m_DbIndex = Math.Min(Math.Max(dbIndex, RedisConstants.MinDbIndex), RedisConstants.MaxDbIndex);
        }

        #endregion Redis Based Methods

        public RedisSocket Accept()
        {
            return new RedisSocket(m_Socket.Accept());
        }

        public bool AcceptAsync(SocketAsyncEventArgs e)
        {
            return m_Socket.AcceptAsync(e);
        }

        public IAsyncResult BeginAccept(int receiveSize, AsyncCallback callback, object state)
        {
            return m_Socket.BeginAccept(receiveSize, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginAccept(RedisSocket acceptSocket, int receiveSize, AsyncCallback callback, object state)
        {
            return m_Socket.BeginAccept(acceptSocket.m_Socket, receiveSize, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginAccept(AsyncCallback callback, object state)
        {
            return m_Socket.BeginAccept(callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginConnect(IPAddress address, int port, AsyncCallback callback, object state)
        {
            var connState = new Tuple<Socket, bool, IPEndPoint>(m_Socket, m_Socket.IsConnected(), new IPEndPoint(address, port));
            return m_Socket.BeginConnect(address, port, callback, new RedisAsyncStateWrapper(state, connState));
        }

        public IAsyncResult BeginDisconnect(bool reuseSocket, AsyncCallback callback, object state)
        {
            var connState = new Tuple<Socket, bool>(m_Socket, m_Socket.IsConnected());
            return m_Socket.BeginDisconnect(reuseSocket, callback, new RedisAsyncStateWrapper(state, connState));
        }

        public IAsyncResult BeginReceive(byte[] buffer, int offset, int size, SocketFlags socket_flags, AsyncCallback callback, object state)
        {
            return m_Socket.BeginReceive(buffer, offset, size, socket_flags, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginReceive(byte[] buffer, int offset, int size, SocketFlags flags, out SocketError error, AsyncCallback callback, object state)
        {
            return m_Socket.BeginReceive(buffer, offset, size, flags, out error, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginReceive(IList<ArraySegment<byte>> buffers, SocketFlags socketFlags, AsyncCallback callback, object state)
        {
            return m_Socket.BeginReceive(buffers, socketFlags, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginReceive(IList<ArraySegment<byte>> buffers, SocketFlags socketFlags, out SocketError errorCode, AsyncCallback callback, object state)
        {
            return m_Socket.BeginReceive(buffers, socketFlags, out errorCode, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginReceiveFrom(byte[] buffer, int offset, int size, SocketFlags socket_flags, ref EndPoint remote_end, AsyncCallback callback, object state)
        {
            return m_Socket.BeginReceiveFrom(buffer, offset, size, socket_flags, ref remote_end, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginReceiveMessageFrom(byte[] buffer, int offset, int size, SocketFlags socketFlags, ref EndPoint remoteEP, AsyncCallback callback, object state)
        {
            return m_Socket.BeginReceiveMessageFrom(buffer, offset, size, socketFlags, ref remoteEP, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginSend(IList<ArraySegment<byte>> buffers, SocketFlags socketFlags, out SocketError errorCode, AsyncCallback callback, object state)
        {
            return m_Socket.BeginSend(buffers, socketFlags, out errorCode, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginSend(byte[] buffer, int offset, int size, SocketFlags socket_flags, AsyncCallback callback, object state)
        {
            return m_Socket.BeginSend(buffer, offset, size, socket_flags, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginSend(IList<ArraySegment<byte>> buffers, SocketFlags socketFlags, AsyncCallback callback, object state)
        {
            return m_Socket.BeginSend(buffers, socketFlags, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginSend(byte[] buffer, int offset, int size, SocketFlags socketFlags, out SocketError errorCode, AsyncCallback callback, object state)
        {
            return m_Socket.BeginSend(buffer, offset, size, socketFlags, out errorCode, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginSendFile(string fileName, AsyncCallback callback, object state)
        {
            return m_Socket.BeginSendFile(fileName, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginSendFile(string fileName, byte[] preBuffer, byte[] postBuffer, TransmitFileOptions flags, AsyncCallback callback, object state)
        {
            return m_Socket.BeginSendFile(fileName, preBuffer, postBuffer, flags, callback, new RedisAsyncStateWrapper(state));
        }

        public IAsyncResult BeginSendTo(byte[] buffer, int offset, int size, SocketFlags socket_flags, EndPoint remote_end, AsyncCallback callback, object state)
        {
            return m_Socket.BeginSendTo(buffer, offset, size, socket_flags, remote_end, callback, new RedisAsyncStateWrapper(state));
        }

        public void Bind(EndPoint local_end)
        {
            m_Socket.Bind(local_end);
        }

        public void Close()
        {
            var onDisconnect = m_OnDisconnect;
            var wasConnected = (onDisconnect != null) && m_Socket.IsConnected();

            m_Socket.Close();

            if (wasConnected && (onDisconnect != null) && !m_Socket.IsConnected())
                onDisconnect(this);
        }

        public void Close(int timeout)
        {
            try
            {
                var onDisconnect = m_OnDisconnect;
                var wasConnected = (onDisconnect != null) && m_Socket.IsConnected();

                m_Socket.Close(timeout);

                if (wasConnected && (onDisconnect != null) && !m_Socket.IsConnected())
                    onDisconnect(this);
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        public void Connect(IPAddress address, int port)
        {
            try
            {
                var onConnect = m_OnConnect;
                var wasDisconnected = (onConnect != null) && !m_Socket.IsConnected();

                m_Socket.Connect(address, port);

                if (!m_Socket.IsConnected())
                    m_RemoteEP = null;
                else
                {
                    m_RemoteEP = new IPEndPoint(address, port);
                    if (wasDisconnected && (onConnect != null))
                        onConnect(this);
                }
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        public bool ConnectAsync(SocketAsyncEventArgs e)
        {
            if (e == null)
                throw new RedisFatalException(new ArgumentNullException("e"), RedisErrorCode.ConnectionError);

            var endPoint = e.RemoteEndPoint;
            if (endPoint == null)
                throw new RedisFatalException(new ArgumentNullException("e.RemoteEndPoint"), RedisErrorCode.ConnectionError);

            if (!(endPoint is IPEndPoint))
                throw new RedisFatalException(new ArgumentNullException("e.RemoteEndPoint", "EndPoint is not in expected form"), RedisErrorCode.ConnectionError);

            e.Completed += OnConnectComplete;

            try
            {
                return m_Socket.ConnectAsync(e);
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        private void OnConnectComplete(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                e.Completed -= OnConnectComplete;

                if (!e.ConnectSocket.IsConnected())
                    m_RemoteEP = null;
                else
                {
                    m_RemoteEP = e.RemoteEndPoint as IPEndPoint;

                    var onConnect = m_OnConnect;
                    if (onConnect != null)
                        onConnect(this);
                }
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        public void Disconnect(bool reuseSocket)
        {
            var onDisconnect = m_OnDisconnect;
            var wasConnected = (onDisconnect != null) && m_Socket.IsConnected();

            try
            {
                m_Socket.Disconnect(reuseSocket);

                if (wasConnected && (onDisconnect != null) && !m_Socket.IsConnected())
                    onDisconnect(this);
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        public bool DisconnectAsync(SocketAsyncEventArgs e)
        {
            if (e == null)
                throw new RedisFatalException(new ArgumentNullException("e"), RedisErrorCode.ConnectionError);
            e.Completed += OnDisconnectComplete;

            try
            {
                return m_Socket.DisconnectAsync(e);
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        private void OnDisconnectComplete(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                e.Completed -= OnDisconnectComplete;

                var onDisconnect = m_OnDisconnect;
                if ((onDisconnect != null) && !e.ConnectSocket.IsConnected())
                    onDisconnect(this);
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        public SocketInformation DuplicateAndClose(int targetProcessId)
        {
            return m_Socket.DuplicateAndClose(targetProcessId);
        }

        public RedisSocket EndAccept(out byte[] buffer, IAsyncResult asyncResult)
        {
            return new RedisSocket(m_Socket.EndAccept(out buffer, asyncResult));
        }

        public RedisSocket EndAccept(out byte[] buffer, out int bytesTransferred, IAsyncResult asyncResult)
        {
            return new RedisSocket(m_Socket.EndAccept(out buffer, out bytesTransferred, asyncResult));
        }

        public RedisSocket EndAccept(IAsyncResult asyncResult)
        {
            return new RedisSocket(m_Socket.EndAccept(asyncResult));
        }

        public void EndConnect(IAsyncResult asyncResult)
        {
            try
            {
                Tuple<Socket, bool, IPEndPoint> connState = null;

                var wrapper = asyncResult.AsyncState as RedisAsyncStateWrapper;
                if (wrapper != null)
                    connState = wrapper.Tag as Tuple<Socket, bool, IPEndPoint>;

                m_Socket.EndConnect(asyncResult);

                if ((connState == null) || !connState.Item1.IsConnected())
                    m_RemoteEP = null;
                else
                {
                    m_RemoteEP = connState.Item3;
                    if (!connState.Item2)
                    {
                        var onConnect = m_OnConnect;
                        if (onConnect != null)
                            onConnect(this);
                    }
                }
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        public void EndDisconnect(IAsyncResult asyncResult)
        {
            try
            {
                Tuple<Socket, bool> connState = null;

                var wrapper = asyncResult.AsyncState as RedisAsyncStateWrapper;
                if (wrapper != null)
                    connState = wrapper.Tag as Tuple<Socket, bool>;

                m_Socket.EndDisconnect(asyncResult);

                var onDisconnect = m_OnDisconnect;
                if ((onDisconnect != null) && (connState != null) &&
                    connState.Item2 && !connState.Item1.IsConnected())
                    onDisconnect(this);
            }
            catch (Exception ex)
            {
                throw new RedisFatalException(ex, RedisErrorCode.ConnectionError);
            }
        }

        public int EndReceive(IAsyncResult asyncResult)
        {
            return m_Socket.EndReceive(asyncResult);
        }

        public int EndReceive(IAsyncResult asyncResult, out SocketError errorCode)
        {
            return m_Socket.EndReceive(asyncResult, out errorCode);
        }

        public int EndReceiveFrom(IAsyncResult asyncResult, ref EndPoint end_point)
        {
            return m_Socket.EndReceiveFrom(asyncResult, ref end_point);
        }

        public int EndReceiveMessageFrom(IAsyncResult asyncResult, ref SocketFlags socketFlags, ref EndPoint endPoint, out IPPacketInformation ipPacketInformation)
        {
            return m_Socket.EndReceiveMessageFrom(asyncResult, ref socketFlags, ref endPoint, out ipPacketInformation);
        }

        public int EndSend(IAsyncResult asyncResult)
        {
            return m_Socket.EndSend(asyncResult);
        }

        public int EndSend(IAsyncResult asyncResult, out SocketError errorCode)
        {
            return m_Socket.EndSend(asyncResult, out errorCode);
        }

        public void EndSendFile(IAsyncResult asyncResult)
        {
            m_Socket.EndSendFile(asyncResult);
        }

        public int EndSendTo(IAsyncResult asyncResult)
        {
            return m_Socket.EndSendTo(asyncResult);
        }

        private string GetConnectedHost()
        {
            var remoteEP = GetConnectedHost(m_RemoteEP);
            if (remoteEP.IsEmpty())
                remoteEP = GetConnectedHost(LocalEndPoint);
            return remoteEP;
        }

        private static string GetConnectedHost(object remoteEP)
        {
            if (remoteEP != null)
            {
                if (remoteEP is DnsEndPoint)
                    return ((DnsEndPoint)remoteEP).Host;

                if (remoteEP is IPEndPoint)
                {
                    var address = ((IPEndPoint)remoteEP).Address;
                    if (address != null)
                        return address.ToString();
                }
            }
            return null;
        }

        public Stream GetRealStream()
        {
            ValidateNotDisposed();

            var rs = m_RealStream;
            if (rs == null)
            {
                rs = new NetworkStream(m_Socket, false);
                if (UseSsl)
                {
                    var host = GetConnectedHost();
                    if (host.IsEmpty())
                        throw new RedisFatalException("Remote end-point can not be defined for ssl usage", RedisErrorCode.ConnectionError);

                    var ssl = new SslStream(rs, false, m_SslCertificateValidation, m_SslCertificateSelection);

                    ssl.AuthenticateAsClient(host);
                    if (!ssl.IsEncrypted)
                        throw new RedisFatalException("Cannot create encrypted connection, end-point: " + host, RedisErrorCode.ConnectionError);

                    rs = ssl;
                }

                Interlocked.Exchange(ref m_RealStream, rs);
            }
            return rs;
        }

        public Stream GetBufferedStream()
        {
            ValidateNotDisposed();

            var ws = m_BufferedStream;
            if (ws == null)
            {
                ws = new BufferedStream(GetRealStream(), 1024);
                Interlocked.Exchange(ref m_BufferedStream, ws);
            }
            return ws;
        }

        public byte[] GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, int length)
        {
            return m_Socket.GetSocketOption(optionLevel, optionName, length);
        }

        public void GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, byte[] optionValue)
        {
            m_Socket.GetSocketOption(optionLevel, optionName, optionValue);
        }

        public object GetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName)
        {
            return m_Socket.GetSocketOption(optionLevel, optionName);
        }

        public int IOControl(int ioctl_code, byte[] in_value, byte[] out_value)
        {
            return m_Socket.IOControl(ioctl_code, in_value, out_value);
        }

        public int IOControl(IOControlCode ioControlCode, byte[] optionInValue, byte[] optionOutValue)
        {
            return m_Socket.IOControl(ioControlCode, optionInValue, optionOutValue);
        }

        public void Listen(int backlog)
        {
            m_Socket.Listen(backlog);
        }

        public bool Poll(int time_us, SelectMode mode)
        {
            return m_Socket.Poll(time_us, mode);
        }

        public int Receive(IList<ArraySegment<byte>> buffers, SocketFlags socketFlags, out SocketError errorCode)
        {
            return m_Socket.Receive(buffers, socketFlags, out errorCode);
        }

        public int Receive(byte[] buffer)
        {
            return m_Socket.Receive(buffer);
        }

        public int Receive(byte[] buffer, int offset, int size, SocketFlags flags)
        {
            return m_Socket.Receive(buffer, offset, size, flags);
        }

        public int Receive(byte[] buffer, int offset, int size, SocketFlags flags, out SocketError error)
        {
            return m_Socket.Receive(buffer, offset, size, flags, out error);
        }

        public int Receive(byte[] buffer, int size, SocketFlags flags)
        {
            return m_Socket.Receive(buffer, size, flags);
        }

        public int Receive(byte[] buffer, SocketFlags flags)
        {
            return m_Socket.Receive(buffer, flags);
        }

        public int Receive(IList<ArraySegment<byte>> buffers)
        {
            return m_Socket.Receive(buffers);
        }

        public int Receive(IList<ArraySegment<byte>> buffers, SocketFlags socketFlags)
        {
            return m_Socket.Receive(buffers, socketFlags);
        }

        public bool ReceiveAsync(SocketAsyncEventArgs e)
        {
            return m_Socket.ReceiveAsync(e);
        }

        public int ReceiveFrom(byte[] buffer, int size, SocketFlags flags, ref EndPoint remoteEP)
        {
            return m_Socket.ReceiveFrom(buffer, size, flags, ref remoteEP);
        }

        public int ReceiveFrom(byte[] buffer, ref EndPoint remoteEP)
        {
            return m_Socket.ReceiveFrom(buffer, ref remoteEP);
        }

        public int ReceiveFrom(byte[] buffer, SocketFlags flags, ref EndPoint remoteEP)
        {
            return m_Socket.ReceiveFrom(buffer, flags, ref remoteEP);
        }

        public int ReceiveFrom(byte[] buffer, int offset, int size, SocketFlags flags, ref EndPoint remoteEP)
        {
            return m_Socket.ReceiveFrom(buffer, offset, size, flags, ref remoteEP);
        }

        public bool ReceiveFromAsync(SocketAsyncEventArgs e)
        {
            return m_Socket.ReceiveFromAsync(e);
        }

        public int ReceiveMessageFrom(byte[] buffer, int offset, int size, ref SocketFlags socketFlags, ref EndPoint remoteEP, out IPPacketInformation ipPacketInformation)
        {
            return m_Socket.ReceiveMessageFrom(buffer, offset, size, ref socketFlags, ref remoteEP, out ipPacketInformation);
        }

        public bool ReceiveMessageFromAsync(SocketAsyncEventArgs e)
        {
            return m_Socket.ReceiveMessageFromAsync(e);
        }

        public int Send(IList<ArraySegment<byte>> buffers, SocketFlags socketFlags)
        {
            return m_Socket.Send(buffers, socketFlags);
        }

        public int Send(IList<ArraySegment<byte>> buffers)
        {
            return m_Socket.Send(buffers);
        }

        public int Send(byte[] buf, SocketFlags flags)
        {
            return m_Socket.Send(buf, flags);
        }

        public int Send(IList<ArraySegment<byte>> buffers, SocketFlags socketFlags, out SocketError errorCode)
        {
            return m_Socket.Send(buffers, socketFlags, out errorCode);
        }

        public int Send(byte[] buf, int offset, int size, SocketFlags flags, out SocketError error)
        {
            return m_Socket.Send(buf, offset, size, flags, out error);
        }

        public int Send(byte[] buf, int offset, int size, SocketFlags flags)
        {
            return m_Socket.Send(buf, offset, size, flags);
        }

        public int Send(byte[] buf)
        {
            return m_Socket.Send(buf);
        }

        public int Send(byte[] buf, int size, SocketFlags flags)
        {
            return m_Socket.Send(buf, size, flags);
        }

        public bool SendAsync(SocketAsyncEventArgs e)
        {
            return m_Socket.SendAsync(e);
        }

        public void SendFile(string fileName)
        {
            m_Socket.SendFile(fileName);
        }

        public void SendFile(string fileName, byte[] preBuffer, byte[] postBuffer, TransmitFileOptions flags)
        {
            m_Socket.SendFile(fileName, preBuffer, postBuffer, flags);
        }

        public bool SendPacketsAsync(SocketAsyncEventArgs e)
        {
            return m_Socket.SendPacketsAsync(e);
        }

        public int SendTo(byte[] buffer, SocketFlags flags, EndPoint remote_end)
        {
            return m_Socket.SendTo(buffer, flags, remote_end);
        }

        public int SendTo(byte[] buffer, EndPoint remote_end)
        {
            return m_Socket.SendTo(buffer, remote_end);
        }

        public int SendTo(byte[] buffer, int offset, int size, SocketFlags flags, EndPoint remote_end)
        {
            return m_Socket.SendTo(buffer, offset, size, flags, remote_end);
        }

        public int SendTo(byte[] buffer, int size, SocketFlags flags, EndPoint remote_end)
        {
            return m_Socket.SendTo(buffer, size, flags, remote_end);
        }

        public bool SendToAsync(SocketAsyncEventArgs e)
        {
            return m_Socket.SendToAsync(e);
        }

        public void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, object optionValue)
        {
            m_Socket.SetSocketOption(optionLevel, optionName, optionValue);
        }

        public void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, bool optionValue)
        {
            m_Socket.SetSocketOption(optionLevel, optionName, optionValue);
        }

        public void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, byte[] optionValue)
        {
            m_Socket.SetSocketOption(optionLevel, optionName, optionValue);
        }

        public void SetSocketOption(SocketOptionLevel optionLevel, SocketOptionName optionName, int optionValue)
        {
            m_Socket.SetSocketOption(optionLevel, optionName, optionValue);
        }

        public void Shutdown(SocketShutdown how)
        {
            m_Socket.Shutdown(how);
        }

        #endregion Methods
    }
}
