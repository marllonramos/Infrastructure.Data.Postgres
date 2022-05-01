using Infrastructure.Data.Interface;
using Infrastructure.Data.Postgres.Interface;
using System.Data;

namespace Infrastructure.Data.Postgres.Database
{
    public class PostgresDatabaseContext : IPostgresDatabaseContext
    {
        public IDbConnection Connection { get; private set; }
        public IDbTransaction Transaction { get; private set; }
        public IDatabase Database { get; private set; }

        public PostgresDatabaseContext(IPostgresDatabase database) => Database = database;

        public void BeginTransaction()
        {
            Connection = Database.GetConnection();
            if (Connection.State != ConnectionState.Open)
                Connection.Open();

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
                if (Connection.State == ConnectionState.Open)
                    Connection.Close();

                Connection.Dispose();
                Connection = null;
            }
        }
    }
}
