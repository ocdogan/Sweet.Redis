namespace Sweet.Redis
{
    public class RedisDouble : RedisResult<double>
    {
        #region .Ctors

        internal RedisDouble()
        { }

        internal RedisDouble(double value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Double; } }

        #endregion Properties
    }
}
