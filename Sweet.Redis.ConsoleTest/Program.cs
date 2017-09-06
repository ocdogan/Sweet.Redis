using System;
using System.Diagnostics;
using System.Text;

using Sweet.Redis;

namespace Sweet.Redis.ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1, idleTimeout: 5)))
            {
                using (var db = pool.GetDb())
                {
                    db.Strings.Set("large_text", largeText);
                }

                do
                {
                    Console.Clear();
                    try
                    {
                        /* using (var db = pool.GetDb())
                        {
                            WriteResult("Ping, ", db.Connection.Ping());
                            WriteResult("Ping 1, ", db.Connection.Ping("1"));
                            WriteResult("Ping String.Empty, ", db.Connection.Ping(String.Empty));
                            WriteResult("Echo 1, ", db.Connection.Echo("1"));
                            WriteResult("Echo String.Empty, ", db.Connection.Echo(String.Empty));
                            WriteResult("LastSave, ", db.Server.LastSave());
                            WriteResult("Time, ", db.Server.Time());

                            var cfg = db.Server.ConfigGet("*");
                            if (cfg == null || cfg.Count == 0)
                                WriteResult("ConfigGet *, ", "(null)");
                            else
                            {
                                WriteResult("ConfigGet *, ", "*");

                                var i = 1;
                                foreach (var kv in cfg)
                                {
                                    WriteResult("ConfigGet " + i++ + ") ",
                                                kv.Key + " = " + kv.Value);
                                }
                            }

                            var clients = db.Server.ClientListDictionary();
                            if (clients == null || clients.Length == 0)
                                WriteResult("ClientList, ", "(null)");
                            else
                            {
                                var i = 0;
                                foreach (var client in clients)
                                {
                                    i++;

                                    var j = 1;
                                    foreach (var kv in client)
                                    {
                                        WriteResult("ClientList " + i + ", " + j++ + ") ",
                                                    kv.Key + " = " + kv.Value);
                                    }
                                }
                            }
                        } */

                        /* using (var db = pool.GetDb(1))
                        {
                            WriteResult("Set key1 Hello, ", db.Strings.Set("key1", "Hello"));
                            WriteResult("Set key2 World, ", db.Strings.Set("key2", "World"));
                            WriteResult("Set key3 Hello\\r\\nWorld, ", db.Strings.Set("key3", "Hello\r\nWorld"));
                            WriteResult("Set key4 largeText, ", db.Strings.Set("key4", largeText));

                            WriteResult("GetString key1, ", db.Strings.GetString("key1"));
                            WriteResult("GetString key2, ", db.Strings.GetString("key2"));
                            WriteResult("GetString key3, ", db.Strings.GetString("key3"));
                            // WriteResult("GetString key4 largeText, ", db.Strings.GetString("key4"));

                            ConsoleWriteMultiline("MGet key1, ", db.Strings.MGet("key1"));
                            ConsoleWriteMultiline("MGet key1 key2, ", db.Strings.MGet("key1", "key2"));
                            ConsoleWriteMultiline("MGet key1 key2 key3, ", db.Strings.MGet("key1", "key2", "key3"));
							// ConsoleWriteMultiline("MGet key1 key2 key3 key4, ", db.Strings.MGet("key1", "key2", "key3", "key4"));
						} */

                        var sw = new Stopwatch();
                        using (var db = pool.GetDb())
                        {
                            sw.Restart();

                            for (var i = 0; i < 1000; i++)
                                db.Strings.Get("large_text");

                            /* for (var i = 0; i < 1000; i++)
                                db.Connection.Ping(); */

                            /* {
                                Console.WriteLine((i + 1).ToString() + ") ");
								try
                                {
                                    Console.WriteLine((i + 1).ToString() + ") " + db.Connection.Ping());
                                    // Console.WriteLine(db.Strings.GetString("key4"));
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e);
                                }
								Console.WriteLine();
							} */
                        }

                        sw.Stop();
                        Console.WriteLine("Elleapsed time: " + sw.ElapsedMilliseconds + " msec");
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }

                    Console.WriteLine();
                    Console.WriteLine("Press any key to continue, ESC to escape ...");
                }
                while (Console.ReadKey(true).Key != ConsoleKey.Escape);
            }
        }

        private static void WriteResult(string cmd, object result)
        {
            var str = (result == null) ? String.Empty : (result.ToString() ?? String.Empty);

            Console.Write(cmd);
            Console.WriteLine(str);
            Console.WriteLine("Length: " + str.Length);
            Console.WriteLine("-----------------------------------------------");
            Console.WriteLine();
        }

        private static void ConsoleWriteMultiline(string cmd, string[] strArray)
        {
            Console.Write(cmd);
            if (strArray == null)
                Console.WriteLine("1..) (null)");
            else
            {
                for (var i = 0; i < strArray.Length; i++)
                {
                    var str = strArray[i];
                    if (str == null)
                        Console.Write((i + 1) + ") (null)");
                    else
                        Console.Write((i + 1) + ") " + str);

                    if (i < strArray.Length - 1)
                        Console.Write(", ");

                    Console.WriteLine();
                }
            }
            Console.WriteLine("-----------------------------------------------");
            Console.WriteLine();
        }
    }
}
