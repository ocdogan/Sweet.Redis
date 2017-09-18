namespace Sweet.Redis
{
    public class RedisError : RedisResult<string>
    {
        #region .Ctors

        internal RedisError()
        { }

        internal RedisError(string value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Error; } }

        #endregion Properties
    }
}
