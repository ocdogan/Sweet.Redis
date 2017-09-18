namespace Sweet.Redis
{
    public class RedisObj : RedisResult<object>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Object; } }

        #endregion Properties
    }
}
