using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ICSharpCode.SharpZipLib.BZip2;
using Mono.Data.Sqlite;
using Newtonsoft.Json.Linq;

namespace RedditDB
{
    class Program
    {
        private static string dbPath = "Experiment1.sqlite";
        private static string bzipPath = "RC_2007-10.bz2";

        static void Main(string[] args)
        {
            if (!File.Exists(dbPath))
            {
                SqliteConnection.CreateFile(dbPath);
            }

            DBProcessor p = new DBProcessor();
            p.Start(dbPath, bzipPath);
        }
    }
}
