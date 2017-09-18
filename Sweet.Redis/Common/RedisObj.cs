namespace Sweet.Redis
{
    public class RedisObj : RedisResult<object>
    {
        #region .Ctors

        internal RedisObj()
        { }

        internal RedisObj(object value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Object; } }

        #endregion Properties
    }
}
