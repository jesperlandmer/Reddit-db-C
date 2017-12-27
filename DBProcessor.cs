using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using ICSharpCode.SharpZipLib.BZip2;
using Newtonsoft.Json.Linq;

namespace RedditDB
{
    public class DBProcessor
    {
        private DBHelper _db;
        private int counter = 0;
        private dynamic _watch;

        public bool Start(string dbPath, string bzipPath)
        {
            System.Console.WriteLine("Start Process[s], Drop Tables[d], Quit[q]");

            _db = new DBHelper(dbPath);
            _db.OpenConnection();

            switch (System.Console.In.ReadLine())
            {
                case "s":
                    StartTimer();
                    _db.CreateTables();
                    CreateDatabase(bzipPath, _db);
                    Console.WriteLine("Elapsed time: " + ElapsedTime() + " seconds");
                    Console.WriteLine("\nObjects written: " + counter);
                    _db.CloseConnection();
                    return true;
                case "d":
                    _db.DropTables();
                    _db.CloseConnection();
                    return true;
                case "q":
                    _db.CloseConnection();
                    return false;
                default:
                    _db.CloseConnection();
                    throw new Exception("Invalid option");
            }
        }

        private void CreateDatabase(string bzipPath, DBHelper db)
        {
            string decompressedFile = GetDecompressedFile(bzipPath);
            using (StreamReader sr = new StreamReader(decompressedFile))
            {
                while (sr.ReadLine() != null)
                {
                    try 
                    {
                        db.InsertToDB(JObject.Parse(sr.ReadLine()));
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(sr.ReadLine());
                    }
                }
            }

        File.Delete(decompressedFile);
    }
    private string GetDecompressedFile(string path)
    {
        string tempPath = Path.GetRandomFileName();
        FileStream fs = new FileStream(path, FileMode.Open);

        using (FileStream decompressedStream = File.Create(tempPath))
        {
           BZip2.Decompress(fs, decompressedStream, true);
        }

        return tempPath;
    }

    private void StartTimer()
    {
        _watch = System.Diagnostics.Stopwatch.StartNew();
    }
    private long ElapsedTime()
    {
        _watch.Stop();
        return _watch.ElapsedMilliseconds / 1000;
    }
}
}
