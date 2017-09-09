namespace Sweet.Redis
{
    public interface IRedisSetsCommands
    {
        long SAdd(string key, byte[] member, params byte[][] members);
        long SAdd(string key, string member, params string[] members);

        long SCard(string key);

        byte[][] SDiff(string fromKey, params string[] keys);
        long SDiffStore(string toKey, string fromKey, params string[] keys);
        string[] SDiffString(string fromKey, params string[] keys);

        byte[][] SInter(string key, params string[] keys);
        long SInterStore(string toKey, params string[] keys);
        string[] SInterStrings(string key, params string[] keys);

        bool SIsMember(string key, byte[] member);
        bool SIsMember(string key, string member);

        byte[][] SMembers(string key);
        string[] SMemberStrings(string key);

        bool SMove(string fromKey, string toKey, byte[] member);
        bool SMove(string fromKey, string toKey, string member);

        byte[] SPop(string key);
        string SPopString(string key);

        byte[] SRandMember(string key);
        byte[][] SRandMember(string key, int count);
        string SRandMemberString(string key);
        string[] SRandMemberString(string key, int count);

        long SRem(string key, byte[] member, params byte[][] members);
        long SRem(string key, string member, params string[] members);

        byte[][] SScan(string key, int count = 10, string match = null);
        string[] SScanString(string key, int count = 10, string match = null);

        byte[][] SUnion(string key, params string[] keys);
        long SUnionStore(string intoKey, params string[] keys);
        string[] SUnionStrings(string key, params string[] keys);
    }
}
