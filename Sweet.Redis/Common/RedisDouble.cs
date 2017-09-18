namespace Sweet.Redis
{
    public class RedisDouble : RedisResult<double>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Double; } }

        #endregion Properties
    }
}
