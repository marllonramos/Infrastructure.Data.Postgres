using Infrastructure.Data.Postgres.Interface;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System.Data;

namespace Infrastructure.Data.Postgres.Database
{
    public class PostgresDatabase : IPostgresDatabase
    {
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;

        //public PostgresDatabase() => _connectionString = ConfigurationManager.AppSettings["ConnectionStringPostgres"];
        public PostgresDatabase(IConfiguration configuration)
        {
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("ConnectionStringPostgres");
        }

        public IDbConnection GetConnection() => new NpgsqlConnection(_connectionString);
        public IDbTransaction GetTransaction(IDbConnection connection, IsolationLevel isolationLevel) => connection.BeginTransaction(isolationLevel);


        public DataTable GetData(string query)
        {
            return GetData(null, null, query);
        }

        public DataTable GetData(IDbConnection connection, IDbTransaction transaction, string query)
        {
            DataTable resultTable = null;
            resultTable = GetData(connection, transaction, new NpgsqlCommand(query));
            return resultTable;
        }

        public DataTable GetData(IDbConnection connection, IDbTransaction transaction, IDbCommand command)
        {
            bool closeConnection = false;
            DataTable resultTable = null;

            try
            {
                if (connection == null)
                {
                    connection = GetConnection();
                    if (connection.State != ConnectionState.Open)
                        connection.Open();
                    closeConnection = true;
                }

                command.Connection = (NpgsqlConnection)connection;
                command.CommandType = CommandType.Text;

                if (transaction != null)
                    command.Transaction = (NpgsqlTransaction)transaction;

                var cmd = (NpgsqlCommand)command;
                var da = new NpgsqlDataAdapter(cmd);
                var dt = new DataTable();
                da.Fill(dt);

                resultTable = dt;
            }
            catch
            {
                throw;
            }
            finally
            {
                if (closeConnection)
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                }
            }

            return resultTable;
        }


        public void UpdateData(string commandText)
        {
            UpdateData(null, null, commandText);
        }

        public void UpdateData(IDbConnection connection, IDbTransaction transaction, string commandText)
        {
            UpdateData(connection, transaction, new NpgsqlCommand(commandText));
        }

        public void UpdateData(IDbConnection connection, IDbTransaction transaction, IDbCommand command)
        {
            bool closeConnection = false;

            try
            {
                if (connection == null)
                {
                    connection = GetConnection();
                    if (connection.State != ConnectionState.Open)
                        connection.Open();
                    closeConnection = true;
                }

                command.CommandType = CommandType.Text;
                command.Connection = (NpgsqlConnection)connection;

                if (transaction != null)
                    command.Transaction = (NpgsqlTransaction)transaction;

                command.ExecuteNonQuery();
            }
            catch
            {
                throw;
            }
            finally
            {
                if (closeConnection)
                {
                    if (connection.State == ConnectionState.Open)
                        connection.Close();
                }
            }
        }
    }
}
