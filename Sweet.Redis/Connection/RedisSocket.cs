using System.IO;
using System.Net.Sockets;
using System.Threading;

namespace Sweet.Redis
{
    public class RedisSocket : Socket, IRedisDisposable
    {
        #region Field Members

        private long m_Disposed;
        private NetworkStream m_ReadStream;
        private BufferedStream m_WriteStream;

        #endregion Field Members

        #region .Ctors

        public RedisSocket(SocketInformation socketInformation)
            : base(socketInformation)
        { }

        public RedisSocket(AddressFamily addressFamily, SocketType socketType, ProtocolType protocolType)
            : base(addressFamily, socketType, protocolType)
        { }

        #endregion .Ctors

        #region Destructors

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            var rs = Interlocked.Exchange(ref m_ReadStream, null);
            if (rs != null)
                rs.Dispose();

            var ws = Interlocked.Exchange(ref m_WriteStream, null);
            if (ws != null)
                ws.Dispose();
        }

        #endregion Destructors

        #region Properties

        public bool Disposed
        {
            get { return Interlocked.Read(ref m_Disposed) != 0L; }
        }

        #endregion Properties

        #region Methods

        public virtual void ValidateNotDisposed()
        {
            if (Disposed)
                throw new RedisException(GetType().Name + " is disposed");
        }

        public Stream GetReadStream()
        {
            ValidateNotDisposed();

            var rs = m_ReadStream;
            if (rs == null)
            {
                rs = new NetworkStream(this, false);
                Interlocked.Exchange(ref m_ReadStream, rs);
            }
            return rs;
        }

        public Stream GetWriteStream()
        {
            ValidateNotDisposed();

            var ws = m_WriteStream;
            if (ws == null)
            {
                ws = new BufferedStream(GetReadStream(), 1024);
                Interlocked.Exchange(ref m_WriteStream, ws);
            }
            return ws;
        }

        #endregion Methods
    }
}
