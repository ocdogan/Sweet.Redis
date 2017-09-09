using System;

namespace Sweet.Redis
{
    public interface IRedisWriter
    {
        void Write(char val);
        void Write(short val);
        void Write(int val);
        void Write(long val);
        void Write(ushort val);
        void Write(uint val);
        void Write(ulong val);
        void Write(decimal val);
        void Write(double val);
        void Write(float val);
        void Write(DateTime val);
        void Write(TimeSpan val);
        void Write(string val);
        void Write(byte[] data);
        void Write(byte val);
        void Write(byte[] data, int index, int length);
    }
}
