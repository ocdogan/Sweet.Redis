namespace Sweet.Redis
{
    public class RedisBool : RedisResult<bool>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.Boolean; } }

        #endregion Properties
    }
}
