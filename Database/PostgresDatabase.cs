using Infrastructure.Data.Postgres.Interface;
using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Infrastructure.Data.Postgres.Database
{
    public class PostgresDatabase : IPostgresDatabase
    {
        private readonly IConfiguration _configuration;
        private readonly string _connectionString;

        public PostgresDatabase(IConfiguration configuration)
        {
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("ConnectionStringPostgres");
        }

        public DbConnection GetConnection() => new NpgsqlConnection(_connectionString);

        public DbTransaction GetTransaction(DbConnection connection, IsolationLevel isolationLevel) => connection.BeginTransaction(isolationLevel);

        public DataTable GetData(string query)
        {
            return GetData(null, null, query);
        }

        public DataTable GetData(DbConnection connection, DbTransaction transaction, string query)
        {
            DataTable resultTable = null;
            resultTable = GetData(connection, transaction, new NpgsqlCommand(query));
            return resultTable;
        }

        public DataTable GetData(DbConnection connection, DbTransaction transaction, DbCommand command)
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

        public void UpdateData(string commandText) => UpdateData(null, null, commandText);

        public void UpdateData(DbConnection connection, DbTransaction transaction, string commandText) => UpdateData(connection, transaction, new NpgsqlCommand(commandText));

        public void UpdateData(DbConnection connection, DbTransaction transaction, DbCommand command)
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


        #region Asynchronous methods
        [Obsolete("Deve-se usar o método 'GetConnection()', pois não há a necessidade de instanciar a classe 'NpgsqlConnection' de forma assíncrona.", true)]
        public Task<DbConnection> GetConnectionAsync()
        {
            throw new NotImplementedException();
        }

        public async Task<DbTransaction> GetTransactionAsync(DbConnection connection, IsolationLevel isolationLevel) => await connection.BeginTransactionAsync(isolationLevel);

        public async Task<DataTable> GetDataAsync(string query)
        {
            return await GetDataAsync(null, null, query);
        }

        public async Task<DataTable> GetDataAsync(DbConnection connection, DbTransaction transaction, string query)
        {
            DataTable resultTable = null;
            resultTable = await GetDataAsync(connection, transaction, new NpgsqlCommand(query));
            return resultTable;
        }

        public async Task<DataTable> GetDataAsync(DbConnection connection, DbTransaction transaction, DbCommand command)
        {
            bool closeConnection = false;
            DataTable resultTable = null;

            try
            {
                if (connection == null)
                {
                    connection = GetConnection();
                    if (connection.State != ConnectionState.Open) await connection.OpenAsync();
                    closeConnection = true;
                }

                command.Connection = (NpgsqlConnection)connection;
                command.CommandType = CommandType.Text;

                if (transaction != null) command.Transaction = (NpgsqlTransaction)transaction;

                using var reader = await command.ExecuteReaderAsync();
                resultTable = new DataTable();
                resultTable.Load(reader);   // aqui é síncrono, mas não tem muito o que fazer. Não existe implementação assíncrona da Microsoft, até a data de hoje(09/05/2025), para este caso.
            }
            catch
            {
                throw;
            }
            finally
            {
                if (closeConnection && connection.State == ConnectionState.Open)
                    await connection.CloseAsync();
            }

            return resultTable;
        }

        public async Task UpdateDataAsync(string commandText) => await UpdateDataAsync(null, null, commandText);

        public async Task UpdateDataAsync(DbConnection connection, DbTransaction transaction, string commandText) => await UpdateDataAsync(connection, transaction, new NpgsqlCommand(commandText));

        public async Task UpdateDataAsync(DbConnection connection, DbTransaction transaction, DbCommand command)
        {
            bool closeConnection = false;

            try
            {
                if (connection == null)
                {
                    connection = GetConnection();
                    if (connection.State != ConnectionState.Open) await connection.OpenAsync();
                    closeConnection = true;
                }

                command.CommandType = CommandType.Text;
                command.Connection = (NpgsqlConnection)connection;

                if (transaction != null) command.Transaction = (NpgsqlTransaction)transaction;

                await command.ExecuteNonQueryAsync();
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
                        await connection.CloseAsync();
                }
            }
        }
        #endregion
    }
}
