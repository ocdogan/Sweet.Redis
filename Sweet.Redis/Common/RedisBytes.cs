namespace Sweet.Redis
{
    public class RedisBytes : RedisResult<byte[]>
    {
        #region .Ctors

        internal RedisBytes()
        { }

        internal RedisBytes(byte[] value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Bytes; } }

        #endregion Properties
    }
}
