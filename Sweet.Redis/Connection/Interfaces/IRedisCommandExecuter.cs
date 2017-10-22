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

namespace Sweet.Redis
{
    internal interface IRedisCommandExecuter : IRedisConnectionProvider
    {
        RedisResponse Execute(RedisCommand command, bool throwException = true);
        RedisRaw ExpectArray(RedisCommand command, bool throwException = true);
        RedisString ExpectBulkString(RedisCommand command, bool throwException = true);
        RedisBytes ExpectBulkStringBytes(RedisCommand command, bool throwException = true);
        RedisDouble ExpectDouble(RedisCommand command, bool throwException = true);
        RedisBool ExpectGreaterThanZero(RedisCommand command, bool throwException = true);
        RedisInteger ExpectInteger(RedisCommand command, bool throwException = true);
        RedisMultiBytes ExpectMultiDataBytes(RedisCommand command, bool throwException = true);
        RedisMultiString ExpectMultiDataStrings(RedisCommand command, bool throwException = true);
        RedisVoid ExpectNothing(RedisCommand command, bool throwException = true);
        RedisNullableDouble ExpectNullableDouble(RedisCommand command, bool throwException = true);
        RedisNullableInteger ExpectNullableInteger(RedisCommand command, bool throwException = true);
        RedisBool ExpectOK(RedisCommand command, bool throwException = true);
        RedisBool ExpectOne(RedisCommand command, bool throwException = true);
        RedisBool ExpectSimpleString(RedisCommand command, string expectedResult, bool throwException = true);
        RedisString ExpectSimpleString(RedisCommand command, bool throwException = true);
        RedisBool ExpectSimpleStringBytes(RedisCommand command, byte[] expectedResult, bool throwException = true);
        RedisBytes ExpectSimpleStringBytes(RedisCommand command, bool throwException = true);
    }
}
