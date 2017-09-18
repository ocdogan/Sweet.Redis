namespace Sweet.Redis
{
    public class RedisInt : RedisResult<long>
    {
        #region .Ctors

        internal RedisInt()
        { }

        internal RedisInt(long value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Integer; } }

        #endregion Properties
    }
}
