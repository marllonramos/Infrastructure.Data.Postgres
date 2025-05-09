using Infrastructure.Data.Interface;
using Infrastructure.Data.Postgres.Interface;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Infrastructure.Data.Postgres.Database
{
    public class PostgresDatabaseContext : IPostgresDatabaseContext
    {
        public DbConnection Connection { get; private set; }
        public DbTransaction Transaction { get; private set; }
        public IDatabase Database { get; private set; }

        public PostgresDatabaseContext(IPostgresDatabase database) => Database = database;

        public void BeginTransaction()
        {
            Connection = Database.GetConnection();
            if (Connection.State != ConnectionState.Open) Connection.Open();
            Transaction = Database.GetTransaction(Connection, IsolationLevel.ReadCommitted);
        }

        public void SaveChanges()
        {
            Transaction.Commit();
            Dispose();
        }

        public void RollBack()
        {
            Transaction.Rollback();
            Dispose();
        }

        public void Dispose()
        {
            if (Transaction != null)
            {
                Transaction.Dispose();
                Transaction = null;
            }

            if (Connection != null)
            {
                if (Connection.State == ConnectionState.Open) Connection.Close();
                Connection.Dispose();
                Connection = null;
            }
        }


        #region Asynchronous methods
        public async Task BeginTransactionAsync()
        {
            Connection = await Database.GetConnectionAsync();
            if (Connection.State != ConnectionState.Open) await Connection.OpenAsync();
            Transaction = await Database.GetTransactionAsync(Connection, IsolationLevel.ReadCommitted);
        }

        public async Task SaveChangesAsync()
        {
            await Transaction.CommitAsync();
            await DisposeAsync();
        }

        public async Task RollBackAsync()
        {
            await Transaction.RollbackAsync();
            await DisposeAsync();
        }

        private async Task DisposeAsync()
        {
            if (Transaction != null)
            {
                await Transaction.DisposeAsync();
                Transaction = null;
            }

            if (Connection != null)
            {
                if (Connection.State == ConnectionState.Open) await Connection.CloseAsync();
                await Connection.DisposeAsync();
                Connection = null;
            }
        }
        #endregion
    }
}
