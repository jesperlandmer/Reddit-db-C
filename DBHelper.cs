using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Mono.Data.Sqlite;
using Newtonsoft.Json.Linq;

namespace RedditDB
{
    public class DBHelper
    {
        private string _dbPath;
        private SqliteConnection _connection;
        private SqliteCommand _command;

        const string SUBREDDITS = "subreddits";
        const string COMMENTS = "comments";

        const string SUBJECTS_TABLE =
            "CREATE TABLE " + SUBREDDITS + "(" +
            "subreddit_id TEXT PRIMARY KEY," +
            "name TEXT NULL);";
        const string COMMENTS_TABLE =
            "CREATE TABLE " + COMMENTS + "(" +
            "id TEXT PRIMARY KEY," +
            "parent_id TEXT NULL," +
            "link_id TEXT NULL," +
            "name TEXT NULL," +
            "author TEXT NULL," +
            "body TEXT NULL," +
            "subreddit TEXT NOT NULL," +
            "score INTEGER NULL," +
            "created_utc TEXT NULL," +
            "FOREIGN KEY(subreddit) REFERENCES subreddits(subreddit_id));";

        public DBHelper(string dbPath)
        {
            _dbPath = dbPath;
            _connection = new SqliteConnection("Data Source=" + dbPath);
            _command = _connection.CreateCommand();
        }

        public void OpenConnection()
        {
            _connection.Open();
        }

        public void CloseConnection()
        {
            _connection.Close();
        }

        public void InsertToDB(JObject obj)
        {
            var commands = new List<string>();

            if (SubredditExists((string)obj["subreddit_id"]) == false)
            {
                commands.Add(@"INSERT INTO [" + SUBREDDITS + "] VALUES(@subreddit_id, @subreddit); ");
            }

            commands.Add(@"INSERT INTO [" + COMMENTS + "] VALUES(@id, @parent_id, @link_id, @name, @author, @body, @subreddit_id, @score, @created_utc); ");

            SqliteParameter subreddit = new SqliteParameter("@subreddit");
            SqliteParameter subredditId = new SqliteParameter("@subreddit_id");
            SqliteParameter id = new SqliteParameter("@id");
            SqliteParameter parentId = new SqliteParameter("@parent_id");
            SqliteParameter linkId = new SqliteParameter("@link_id");
            SqliteParameter name = new SqliteParameter("@name");
            SqliteParameter author = new SqliteParameter("@author");
            SqliteParameter body = new SqliteParameter("@body");
            SqliteParameter score = new SqliteParameter("@score");
            SqliteParameter createdUtc = new SqliteParameter("@created_utc");

            _command.Parameters.Add(subreddit);
            _command.Parameters.Add(subredditId);
            _command.Parameters.Add(id);
            _command.Parameters.Add(parentId);
            _command.Parameters.Add(linkId);
            _command.Parameters.Add(name);
            _command.Parameters.Add(author);
            _command.Parameters.Add(body);
            _command.Parameters.Add(score);
            _command.Parameters.Add(createdUtc);

            subreddit.Value = (string)obj["subreddit"];
            subredditId.Value = (string)obj["subreddit_id"];
            id.Value = (string)obj["id"];
            parentId.Value = (string)obj["parent_id"];
            linkId.Value = (string)obj["link_id"];
            name.Value = (string)obj["name"];
            author.Value = (string)obj["author"];
            body.Value = (string)obj["body"];
            score.Value = (string)obj["score"];
            createdUtc.Value = (string)obj["created_utc"];

            ExecuteCommands(commands);
        }
        private bool SubredditExists(string subreddit_id)
        {
            _command.CommandText = "SELECT count(*) FROM " + SUBREDDITS + " WHERE subreddit_id = '" + subreddit_id + "';";
            int count = Convert.ToInt32(_command.ExecuteScalar());

            return count > 0;
        }

        public void DropTables()
        {
            var commands = new List<string>();
            commands.Add("DROP TABLE IF EXISTS [" + SUBREDDITS + "]; ");
            commands.Add("DROP TABLE IF EXISTS [" + COMMENTS + "]; ");

            ExecuteCommands(commands);
        }
        public void CreateTables()
        {
            var commands = new List<string>();
            commands.Add(SUBJECTS_TABLE);
            commands.Add(COMMENTS_TABLE);

            ExecuteCommands(commands);
        }

        public void StartTransaction()
        {
            var commands = new List<string>();
            commands.Add("BEGIN TRANSACTION;");

            ExecuteCommands(commands);
        }
        public void EndTransaction()
        {
            var commands = new List<string>();
            commands.Add("COMMIT;");

            ExecuteCommands(commands);
        }

        private void ExecuteCommands(List<string> commands)
        {             
            foreach (var command in commands)
            {
                try
                {
                    _command.CommandText = command;
                    var rowcount = _command.ExecuteNonQuery();
                }
                catch (SqliteException e)
                {
                    Console.WriteLine(e.Message);
                }
            }

        }
    }
}