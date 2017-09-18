namespace Sweet.Redis
{
    public class RedisRaw : RedisResult<RedisRawObj>
    {
        #region .Ctors

        internal RedisRaw()
        { }

        internal RedisRaw(RedisRawObj value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Raw; } }

        #endregion Properties
    }
}
