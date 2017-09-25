using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

using Sweet.Redis;

namespace Sweet.Redis.ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // PerformanceTest1();
            // PerformanceTest2();

            // ScriptingNoArgsEvalTest();
            // ScriptingShaNoArgsEvalTest();
            // ScriptingWithArgsEvalTest();
            // ScriptingShaWithArgsEvalTest();

            // PubSubTest1();
            // PubSubTest2();
            // PubSubTest3();
            // PubSubTest4();
            // PubSubTest5();
            // PubSubTest6();
            // PubSubTest7();
            // PubSubTest8();
            // PubSubTest9();
            // PubSubTest10();
            // PubSubTest11();
            // PubSubTest12();

            // MultiThreading1();
            // MultiThreading2();
            // MultiThreading3();
            MultiThreading4();

            // MonitorTest1();
            // MonitorTest2();
            // MonitorTest3();

            // Geo1();
        }

        #region Geo

        static void Geo1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        try
                        {
                            Console.Clear();

                            var addResult = db.Geo.GeoAdd("Sicily",
                                          new RedisGeospatialItem(13.361389, 38.115556, "Palermo"),
                                          new RedisGeospatialItem(15.087269, 37.502669, "Catania"));

                            Console.WriteLine("GEOADD Sicily 13.361389 38.115556 \"Palermo\" 15.087269 37.502669 \"Catania\"");
                            Console.WriteLine(addResult);

                            var distResult = db.Geo.GeoDistanceString("Sicily", "Palermo", "Catania");
                            Console.WriteLine("GEODIST Sicily Palermo Catania");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistanceString("Sicily", "Palermo", "Catania", RedisGeoDistanceUnit.Kilometers);
                            Console.WriteLine("GEODIST Sicily Palermo Catania km");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistanceString("Sicily", "Palermo", "Catania", RedisGeoDistanceUnit.Miles);
                            Console.WriteLine("GEODIST Sicily Palermo Catania mi");
                            Console.WriteLine(distResult);

                            distResult = db.Geo.GeoDistanceString("Sicily", "Foo", "Bar");
                            Console.WriteLine("GEODIST Sicily Foo Bar");
                            Console.WriteLine(distResult);

                            var hashResult = db.Geo.GeoHashString("Sicily", "Palermo", "Catania");
                            Console.WriteLine("GEOHASH Sicily Palermo Catania");
                            Console.WriteLine(hashResult);

                            var posResult = db.Geo.GeoPositionString("Sicily", "Palermo", "Catania", "NonExisting");
                            Console.WriteLine("GEOPOS Sicily Palermo Catania NonExisting");
                            Console.WriteLine(posResult);

                            var radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers, withDist: true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHDIST");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                                200, RedisGeoDistanceUnit.Kilometers, withCoord: true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                        200, RedisGeoDistanceUnit.Kilometers, true, true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD WITHDIST");
                            Console.WriteLine(radiusResult);

                            radiusResult = db.Geo.GeoRadius("Sicily", new RedisGeoPosition(15, 37),
                                                            200, RedisGeoDistanceUnit.Kilometers, true, true, true);
                            Console.WriteLine("GEORADIUS Sicily 15 37 200 km WITHCOORD WITHDIST WITHHASH");
                            Console.WriteLine(radiusResult);
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
        }

        #endregion Geo

        #region Multi-Threading

        static void MultiThreading4()
        {
            var smallText = new string('x', 1000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    db.Strings.Set("small_text", smallText);
                    db.Strings.Get("small_text");
                }

                const int ThreadCount = 50;

                var loopIndex = 0;
                List<Thread> thList = null;
                using (var db = pool.GetDb())
                {
                    do
                    {
                        var ticks = 0L;
                        Console.Clear();

                        var oldThList = thList;
                        if (oldThList != null)
                        {
                            for (var i = 0; i < oldThList.Count; i++)
                            {
                                var th = oldThList[i];
                                try
                                {
                                    if (th.IsAlive)
                                        th.Interrupt();
                                }
                                catch (Exception)
                                { }
                            }
                        }

                        thList = new List<Thread>(ThreadCount);
                        try
                        {
                            loopIndex = 0;
                            var ticksLock = new object();

                            for (var i = 0; i < ThreadCount; i++)
                            {
                                var th = new Thread((obj) =>
                                {
                                    var tupple = (Tuple<Thread, IRedisDb, AutoResetEvent>)obj;

                                    var autoReset = tupple.Item3;
                                    try
                                    {
                                        var @this = tupple.Item1;
                                        var rdb = tupple.Item2;

                                        var sw = new Stopwatch();

                                        for (var j = 0; j < 100; j++)
                                        {
                                            sw.Restart();
                                            var data = rdb.Strings.Get("small_text");
                                            sw.Stop();

                                            Console.WriteLine(@this.Name + ": Get, " + sw.ElapsedMilliseconds.ToString("D3") + " msec, " + (data != null && data.Length == smallText.Length ? "OK" : "FAILED"));

                                            lock (ticksLock)
                                            {
                                                ticks += sw.ElapsedTicks;
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        autoReset.Set();
                                    }
                                });

                                th.Name = loopIndex++.ToString("D2") + "." + i.ToString("D2");
                                th.IsBackground = true;
                                thList.Add(th);
                            }

                            var autoResets = new AutoResetEvent[thList.Count];
                            try
                            {
                                for (var i = 0; i < thList.Count; i++)
                                {
                                    var th = thList[i];

                                    var autoReset = new AutoResetEvent(false);
                                    autoResets[i] = autoReset;

                                    th.Start(new Tuple<Thread, IRedisDb, AutoResetEvent>(th, db, autoReset));
                                }
                            }
                            finally
                            {
                                WaitHandle.WaitAll(autoResets);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();

                        Console.WriteLine("Total ticks: " + ticks);
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void MultiThreading3()
        {
            var smallText = new string('x', 1000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 10)))
            {
                using (var db = pool.GetDb())
                {
                    db.Strings.Set("small_text", smallText);
                    db.Strings.Get("small_text");
                }

                const int ThreadCount = 50;

                var loopIndex = 0;
                List<Thread> thList = null;
                using (var db = pool.GetDb())
                {
                    do
                    {
                        var ticks = 0L;
                        Console.Clear();

                        var oldThList = thList;
                        if (oldThList != null)
                        {
                            for (var i = 0; i < oldThList.Count; i++)
                            {
                                var th = oldThList[i];
                                try
                                {
                                    if (th.IsAlive)
                                        th.Interrupt();
                                }
                                catch (Exception)
                                { }
                            }
                        }

                        thList = new List<Thread>(ThreadCount);
                        try
                        {
                            loopIndex = 0;
                            var ticksLock = new object();

                            for (var i = 0; i < ThreadCount; i++)
                            {
                                var th = new Thread((obj) =>
                                {
                                    var tupple = (Tuple<Thread, IRedisDb, AutoResetEvent>)obj;

                                    var autoReset = tupple.Item3;
                                    try
                                    {
                                        var @this = tupple.Item1;
                                        var rdb = tupple.Item2;

                                        var sw = new Stopwatch();

                                        for (var j = 0; j < 100; j++)
                                        {
                                            sw.Restart();
                                            var data = rdb.Strings.Get("small_text");
                                            sw.Stop();

                                            Console.WriteLine(@this.Name + ": Get, " + sw.ElapsedMilliseconds.ToString("D3") + " msec, " + (data != null && data.Length == smallText.Length ? "OK" : "FAILED"));

                                            lock (ticksLock)
                                            {
                                                ticks += sw.ElapsedTicks;
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        autoReset.Set();
                                    }
                                });

                                th.Name = loopIndex++.ToString("D2") + "." + i.ToString("D2");
                                th.IsBackground = true;
                                thList.Add(th);
                            }

                            var autoResets = new AutoResetEvent[thList.Count];
                            try
                            {
                                for (var i = 0; i < thList.Count; i++)
                                {
                                    var th = thList[i];

                                    var autoReset = new AutoResetEvent(false);
                                    autoResets[i] = autoReset;

                                    th.Start(new Tuple<Thread, IRedisDb, AutoResetEvent>(th, db, autoReset));
                                }
                            }
                            finally
                            {
                                WaitHandle.WaitAll(autoResets);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();

                        Console.WriteLine("Total ticks: " + ticks);
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void MultiThreading2()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 10)))
            {
                using (var db = pool.GetDb())
                {
                    db.Strings.Set("large_text", largeText);
                    db.Strings.Get("large_text");
                }

                const int ThreadCount = 50;

                var loopIndex = 0;
                List<Thread> thList = null;
                using (var db = pool.GetDb())
                {
                    do
                    {
                        var ticks = 0L;
                        Console.Clear();

                        var oldThList = thList;
                        if (oldThList != null)
                        {
                            for (var i = 0; i < oldThList.Count; i++)
                            {
                                var th = oldThList[i];
                                try
                                {
                                    if (th.IsAlive)
                                        th.Interrupt();
                                }
                                catch (Exception)
                                { }
                            }
                        }

                        thList = new List<Thread>(ThreadCount);
                        try
                        {
                            loopIndex = 0;
                            var ticksLock = new object();

                            for (var i = 0; i < ThreadCount; i++)
                            {
                                var th = new Thread((obj) =>
                                {
                                    var tupple = (Tuple<Thread, IRedisDb, AutoResetEvent>)obj;

                                    var autoReset = tupple.Item3;
                                    try
                                    {
                                        var @this = tupple.Item1;
                                        var rdb = tupple.Item2;

                                        var sw = new Stopwatch();

                                        for (var j = 0; j < 100; j++)
                                        {
                                            sw.Restart();
                                            var data = rdb.Strings.Get("large_text");
                                            sw.Stop();

                                            Console.WriteLine(@this.Name + ": Get, " + sw.ElapsedMilliseconds.ToString("D3") + " msec, " + (data != null && data.Length == largeText.Length ? "OK" : "FAILED"));

                                            lock (ticksLock)
                                            {
                                                ticks += sw.ElapsedTicks;
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        autoReset.Set();
                                    }
                                });

                                th.Name = loopIndex++.ToString("D2") + "." + i.ToString("D2");
                                th.IsBackground = true;
                                thList.Add(th);
                            }

                            var autoResets = new AutoResetEvent[thList.Count];
                            try
                            {
                                for (var i = 0; i < thList.Count; i++)
                                {
                                    var th = thList[i];

                                    var autoReset = new AutoResetEvent(false);
                                    autoResets[i] = autoReset;

                                    th.Start(new Tuple<Thread, IRedisDb, AutoResetEvent>(th, db, autoReset));
                                }
                            }
                            finally
                            {
                                WaitHandle.WaitAll(autoResets);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();

                        Console.WriteLine("Total ticks: " + ticks);
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        static void MultiThreading1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 10)))
            {
                const int ThreadCount = 30;

                var loopIndex = 0;
                List<Thread> thList = null;
                using (var db = pool.GetDb())
                {
                    do
                    {
                        var ticks = 0L;
                        Console.Clear();

                        var oldThList = thList;
                        if (oldThList != null)
                        {
                            for (var i = 0; i < oldThList.Count; i++)
                            {
                                var th = oldThList[i];
                                try
                                {
                                    if (th.IsAlive)
                                        th.Interrupt();
                                }
                                catch (Exception)
                                { }
                            }
                        }

                        thList = new List<Thread>(ThreadCount);
                        try
                        {
                            loopIndex = 0;
                            var ticksLock = new object();

                            for (var i = 0; i < ThreadCount; i++)
                            {
                                var th = new Thread((obj) =>
                                {
                                    var tupple = (Tuple<Thread, IRedisDb, AutoResetEvent>)obj;

                                    var autoReset = tupple.Item3;
                                    try
                                    {
                                        var @this = tupple.Item1;
                                        var rdb = tupple.Item2;

                                        var sw = new Stopwatch();

                                        for (var j = 0; j < 1000; j++)
                                        {
                                            sw.Restart();
                                            var result = rdb.Connection.Ping();
                                            sw.Stop();

                                            Console.WriteLine(@this.Name + ": Ping, " + sw.ElapsedMilliseconds.ToString("D3") + " msec, " + result);

                                            lock (ticksLock)
                                            {
                                                ticks += sw.ElapsedTicks;
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        autoReset.Set();
                                    }
                                });

                                th.Name = loopIndex++.ToString("D2") + "." + i.ToString("D2");
                                th.IsBackground = true;
                                thList.Add(th);
                            }

                            var autoResets = new AutoResetEvent[thList.Count];
                            try
                            {
                                for (var i = 0; i < thList.Count; i++)
                                {
                                    var th = thList[i];

                                    var autoReset = new AutoResetEvent(false);
                                    autoResets[i] = autoReset;

                                    th.Start(new Tuple<Thread, IRedisDb, AutoResetEvent>(th, db, autoReset));
                                }
                            }
                            finally
                            {
                                WaitHandle.WaitAll(autoResets);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();

                        Console.WriteLine("Total ticks: " + ticks);
                        Console.WriteLine("Press any key to continue, ESC to escape ...");
                    }
                    while (Console.ReadKey(true).Key != ConsoleKey.Escape);
                }
            }
        }

        #endregion Multi-Threading

        #region Monitor Tests

        static void MonitorTest3()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("1");
                }

                Thread.Sleep(1000);

                pool.MonitorChannel.Quit();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("2");
                }

                Thread.Sleep(1000);
                pool.MonitorChannel.Monitor();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping("3");
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void MonitorTest2()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping();
                }

                Thread.Sleep(5000);

                pool.MonitorChannel.Quit();
                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 5; i++)
                        db.Connection.Ping();
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void MonitorTest1()
        {
            var largeText = Encoding.UTF8.GetBytes(new string('x', 100000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.MonitorChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Time: " + m.Time);
                    Console.WriteLine("Client: " + m.ClientInfo);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                });

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        #endregion Monitor Tests

        #region PubSub Tests

        static void PubSubTest12()
        {
            var largeText = Encoding.UTF8.GetBytes(new string('x', 100000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 1000; i++)
                        db.PubSubs.Publish("abc", largeText);
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest11()
        {
            var smallText = Encoding.UTF8.GetBytes(new string('x', 1000));

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(1000);

                using (var db = pool.GetDb())
                {
                    for (var i = 0; i < 1000; i++)
                        db.PubSubs.Publish("abc", smallText);
                }

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest10()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe();

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest9()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xyz");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe("ab*");

                Thread.Sleep(5000);
                pool.PubSubChannel.PUnsubscribe("xy*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest8()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*");

                Thread.Sleep(5000);

                pool.PubSubChannel.PUnsubscribe("ab*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest7()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*", "xy*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest6()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.PSubscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    Console.WriteLine("Pattern: " + m.Pattern);

                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "ab*");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest5()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Thread.Sleep(4000);

                pool.PubSubChannel.Unsubscribe();

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest4()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Thread.Sleep(4000);

                pool.PubSubChannel.Unsubscribe("xyz");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest3()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc", "xyz");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest2()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                pool.PubSubChannel.Subscribe((m) =>
                {
                    Console.WriteLine("Channel: " + m.Channel);
                    if (m.Data != null)
                        Console.WriteLine("Received data: " + Encoding.UTF8.GetString((byte[])m.Data));
                    else
                        Console.WriteLine("Received data: ?");

                    Console.WriteLine();
                    Console.WriteLine("Press any key to escape ...");
                }, "abc");

                Console.WriteLine();
                Console.WriteLine("Press any key to escape ...");

                Console.ReadKey();
            }
        }

        static void PubSubTest1()
        {
            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            Console.WriteLine("Channels:");

                            var result = db.PubSubs.PubSubChannels();
                            if (result == null || result.Length == 0)
                                Console.WriteLine("(nil)");
                            else
                            {
                                for (var i = 0; i < result.Length; i++)
                                {
                                    var channel = result[i];
                                    Console.WriteLine((i + 1) + ") " + (String.IsNullOrEmpty(channel) ? "(nil)" : channel));
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        try
                        {
                            Console.WriteLine("Numer of Subscribers:");

                            var result = db.PubSubs.PubSubNumerOfSubscribers("xyz");
                            if (result == null)
                                Console.WriteLine("(nil)");
                            else
                            {
                                var value = result.Value;
                                if (value == null || value.Length == 0)
                                    Console.WriteLine("(nil)");
                                else
                                {
                                    for (var i = 0; i < value.Length; i++)
                                    {
                                        var channel = value[i];
                                        Console.WriteLine((i + 1) + ") " + channel.Key + ": " + channel.Value);
                                    }
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }

                        Console.WriteLine();
                        try
                        {
                            Console.Write("Numer of Pattern Subscribers: ");
                            Console.WriteLine(db.PubSubs.PubSubNumerOfSubscriptionsToPatterns());
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
        }

        #endregion PubSub Tests

        #region Scripting Tests

        static void ScriptingNoArgsEvalTest()
        {
            var script = "return {1,2,3.3333,'foo',nil,'bar'}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.Eval(script);
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
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
        }

        static void ScriptingWithArgsEvalTest()
        {
            var script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalString(script,
                                                                 new RedisKeyValue<string, string>("key1", "first"),
                                                                 new RedisKeyValue<string, string>("key2", "second"));
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
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
        }

        static void ScriptingShaNoArgsEvalTest()
        {
            var script = "return {1,2,3.3333,'foo',nil,'bar'}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    var sha1 = db.Scripting.ScriptLoad(script);
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalSHA(sha1);
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
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
        }

        static void ScriptingShaWithArgsEvalTest()
        {
            var script = "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}";

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    var sha1 = db.Scripting.ScriptLoad(script);
                    do
                    {
                        Console.Clear();
                        try
                        {
                            var result = db.Scripting.EvalSHAString(sha1,
                                                                 new RedisKeyValue<string, string>("key1", "first"),
                                                                 new RedisKeyValue<string, string>("key2", "second"));
                            Console.WriteLine((result == null) ? "(nil)" : result.ToString());
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
        }

        #endregion Scripting Tests

        #region Performance Tests

        static void PerformanceTest2()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    db.Strings.Set("large_text", largeText);
                    db.Strings.Get("large_text");
                }

                do
                {
                    Console.Clear();
                    try
                    {
                        var sw = new Stopwatch();
                        using (var db = pool.GetDb())
                        {
                            sw.Restart();

                            for (var i = 0; i < 1000; i++)
                                db.Strings.Get("large_text");
                        }

                        sw.Stop();
                        Console.WriteLine("Elleapsed time: " + sw.ElapsedMilliseconds.ToString("D3") + " msec");
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

        static void PerformanceTest1()
        {
            var largeText = new string('x', 100000);

            using (var pool = new RedisConnectionPool("My redis pool",
                    new RedisSettings(host: "127.0.0.1", port: 6379, maxCount: 1)))
            {
                using (var db = pool.GetDb())
                {
                    db.Connection.Ping();
                }

                do
                {
                    Console.Clear();
                    try
                    {
                        var sw = new Stopwatch();
                        using (var db = pool.GetDb())
                        {
                            sw.Restart();

                            for (var i = 0; i < 1000; i++)
                                db.Connection.Ping();
                        }

                        sw.Stop();
                        Console.WriteLine("Elleapsed time: " + sw.ElapsedMilliseconds.ToString("D3") + " msec");
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

        #endregion Performance Tests

        #region Generic

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

        #endregion Generic
    }
}
