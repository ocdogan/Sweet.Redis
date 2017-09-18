namespace Sweet.Redis
{
    public class RedisBool : RedisResult<bool>
    {
        #region .Ctors

        internal RedisBool()
        { }

        internal RedisBool(bool value)
            : base(value)
        { }

        #endregion .Ctors

        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Boolean; } }

        #endregion Properties
    }
}
