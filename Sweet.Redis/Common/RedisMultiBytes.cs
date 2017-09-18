namespace Sweet.Redis
{
    public class RedisMultiBytes : RedisResult<byte[][]>
    {
        #region .Ctors

        internal RedisMultiBytes()
        { }

        internal RedisMultiBytes(byte[][] value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.MultiBytes; } }

        #endregion Properties
    }
}
