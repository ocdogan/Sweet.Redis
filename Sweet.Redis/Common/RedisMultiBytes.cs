namespace Sweet.Redis
{
    public class RedisMultiBytes : RedisResult<byte[][]>
    {
        #region Properties

        public override RedisResultType Type { get { return RedisResultType.MultiBytes; } }

        #endregion Properties
    }
}
