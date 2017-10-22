#region License
//  The MIT License (MIT)
//
//  Copyright (c) 2017, Cagatay Dogan
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//      The above copyright notice and this permission notice shall be included in
//      all copies or substantial portions of the Software.
//
//      THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//      IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//      FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//      AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//      LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//      OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//      THE SOFTWARE.
#endregion License

using System;
using System.Threading;
using System.Text;

namespace Sweet.Redis
{
    internal class RedisAdmin : RedisPoolClient, IRedisAdmin, IRedisPoolClient
    {
        #region Field Members

        private IRedisServerCommands m_Commands;

        #endregion Field Members

        #region .Ctors

        public RedisAdmin(RedisConnectionPool pool, bool throwOnError = true)
            : base(pool, throwOnError)
        { }

        #endregion .Ctors

        #region Properties

        public IRedisServerCommands Commands
        {
            get
            {
                ValidateNotDisposed();
                if (m_Commands == null)
                    m_Commands = new RedisServerCommands(this);
                return m_Commands;
            }
        }

        #endregion Properties

        #region Methods

        #region Execution Methods

        protected internal override T Expect<T>(RedisCommand command, RedisCommandExpect expectation, string okIf = null)
        {
            switch (expectation)
            {
                case RedisCommandExpect.Response:
                    return (T)(object)Pool.Execute(command, ThrowOnError);
                case RedisCommandExpect.Array:
                    return (T)(object)Pool.ExpectArray(command, ThrowOnError);
                case RedisCommandExpect.BulkString:
                    return (T)(object)Pool.ExpectBulkString(command, ThrowOnError);
                case RedisCommandExpect.BulkStringBytes:
                    return (T)(object)Pool.ExpectBulkStringBytes(command, ThrowOnError);
                case RedisCommandExpect.Double:
                    return (T)(object)Pool.ExpectDouble(command, ThrowOnError);
                case RedisCommandExpect.GreaterThanZero:
                    return (T)(object)Pool.ExpectInteger(command, ThrowOnError);
                case RedisCommandExpect.Integer:
                    return (T)(object)Pool.ExpectInteger(command, ThrowOnError);
                case RedisCommandExpect.MultiDataBytes:
                    return (T)(object)Pool.ExpectMultiDataBytes(command, ThrowOnError);
                case RedisCommandExpect.MultiDataStrings:
                    return (T)(object)Pool.ExpectMultiDataStrings(command, ThrowOnError);
                case RedisCommandExpect.Nothing:
                    return (T)(object)Pool.ExpectNothing(command, ThrowOnError);
                case RedisCommandExpect.NullableDouble:
                    return (T)(object)Pool.ExpectNullableDouble(command, ThrowOnError);
                case RedisCommandExpect.NullableInteger:
                    return (T)(object)Pool.ExpectNullableInteger(command, ThrowOnError);
                case RedisCommandExpect.OK:
                    return (T)(object)Pool.ExpectOK(command, ThrowOnError);
                case RedisCommandExpect.One:
                    return (T)(object)Pool.ExpectOne(command, ThrowOnError);
                case RedisCommandExpect.SimpleString:
                    return (T)(object)Pool.ExpectSimpleString(command, ThrowOnError);
                case RedisCommandExpect.SimpleStringBytes:
                    return (T)(object)Pool.ExpectSimpleStringBytes(command, ThrowOnError);
                default:
                    throw new RedisException(String.Format("Undefined expectation type, {0}", expectation.ToString("F")), RedisErrorCode.NotSupported);
            }
        }

        #endregion Execution Methods

        #endregion Methods
    }
}
