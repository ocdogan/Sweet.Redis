namespace Sweet.Redis
{
    public class RedisString : RedisResult<string>
    {
        #region .Ctors

        internal RedisString()
        { }

        internal RedisString(string value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.String; } }

        #endregion Properties
    }
}
