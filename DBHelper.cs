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

        const string USERS = "users";
        const string SUBREDDITS = "subreddits";
        const string POSTS = "posts";

        const string USERS_TABLE =
            "CREATE TABLE [" + USERS + "] (username NOT NULL PRIMARY KEY); ";
        const string SUBJECTS_TABLE =
            "CREATE TABLE " + SUBREDDITS + "(" +
            "subreddit_id NOT NULL PRIMARY KEY," +
            "subreddit NOT NULL);";
        const string POSTS_TABLE =
            "CREATE TABLE " + POSTS + "(" +
            "id NOT NULL," +
            "parent_id NOT NULL," +
            "link_id NOT NULL," +
            "name NOT NULL," +
            "author NULL," +
            "body TEXT NULL," +
            "subject NOT NULL," +
            "score INT NOT NULL," +
            "created_utc NOT NULL," +
            "FOREIGN KEY(author) REFERENCES users(username)," +
            "FOREIGN KEY(subject) REFERENCES subreddits(subreddit_id));";

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

            if (UserExists((string)obj["author"]) == false && (string)obj["author"] != "[deleted]")
            {
                commands.Add(@"INSERT INTO [" + USERS + "] VALUES(@author); ");
            }

            if (SubredditExists((string)obj["subreddit_id"]) == false)
            {
                commands.Add(@"INSERT INTO [" + SUBREDDITS + "] VALUES(@subreddit_id, @subreddit); ");
            }

            commands.Add(@"INSERT INTO [" + POSTS + "] VALUES(@id, @parent_id, @link_id, @name, @author, @body, @subject, @score, @created_utc); ");

            SqliteParameter subreddit = new SqliteParameter("@subreddit");
            SqliteParameter subredditId = new SqliteParameter("@subreddit_id");

            subreddit.Value = (string)obj["subreddit"];
            subredditId.Value = (string)obj["subreddit_id"];

            SqliteParameter id = new SqliteParameter("@id");
            SqliteParameter parentId = new SqliteParameter("@parent_id");
            SqliteParameter linkId = new SqliteParameter("@link_id");
            SqliteParameter name = new SqliteParameter("@name");
            SqliteParameter author = new SqliteParameter("@author");
            SqliteParameter body = new SqliteParameter("@body");
            SqliteParameter subject = new SqliteParameter("@subject");
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
            _command.Parameters.Add(subject);
            _command.Parameters.Add(score);
            _command.Parameters.Add(createdUtc);

            id.Value = (string)obj["id"];
            parentId.Value = (string)obj["parent_id"];
            linkId.Value = (string)obj["link_id"];
            name.Value = (string)obj["name"];
            author.Value = (string)obj["author"];
            body.Value = (string)obj["body"];
            subject.Value = (string)obj["subreddit_id"];
            score.Value = (string)obj["score"];
            createdUtc.Value = (string)obj["created_utc"];

            ExecuteCommands(commands);
        }
        private bool UserExists(string author)
        {
            _command.CommandText = "SELECT count(*) FROM " + USERS + " WHERE username = '" + author + "';";
            int count = Convert.ToInt32(_command.ExecuteScalar());

            return count > 0;
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
            commands.Add("DROP TABLE IF EXISTS [" + USERS + "]; ");
            commands.Add("DROP TABLE IF EXISTS [" + SUBREDDITS + "]; ");
            commands.Add("DROP TABLE IF EXISTS [" + POSTS + "]; ");

            ExecuteCommands(commands);
        }
        public void CreateTables()
        {
            var commands = new List<string>();
            commands.Add(USERS_TABLE);
            commands.Add(SUBJECTS_TABLE);
            commands.Add(POSTS_TABLE);

            ExecuteCommands(commands);
        }

        private void ExecuteCommands(List<string> commands)
        {
            var transaction = _connection.BeginTransaction();
                                
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

            transaction.Commit();
        }
    }
}