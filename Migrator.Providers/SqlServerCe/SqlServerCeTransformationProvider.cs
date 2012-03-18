using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlServerCe;
using System.IO;
using Migrator.Framework;
using Migrator.Providers.SqlServer;

namespace Migrator.Providers.SqlServerCe
{
    /// <summary>
    ///   Migration transformations provider for Microsoft SQL Server Compact Edition.
    /// </summary>
    public class SqlServerCeTransformationProvider : SqlServerTransformationProvider
    {
        public SqlServerCeTransformationProvider(string connectionString)
            : base(connectionString)
        {
        }

        public override Dialect Dialect
        {
            get { return new SqlServerCeDialect(); }
        }

        protected override void CreateConnection()
        {
            Connection = new SqlCeConnection(ConnectionString);

            if (!File.Exists(Connection.Database))
            {
                CreateDatabaseFile();
            }

            Connection.Open();
        }

        protected string GetSchemaName(string longTableName)
        {
            throw new NotSupportedException("SQL CE does not support database schemas.");
        }


        public override List<string> GetDatabases()
        {
            return new List<string>();
        }


        public override bool ColumnExists(string table, string column)
        {
            if (!TableExists(table))
            {
                return false;
            }
            int firstIndex = table.IndexOf(".");
            if (firstIndex >= 0)
            {
                table = table.Substring(firstIndex + 1);
            }

            using (
                IDataReader reader =
                    base.ExecuteQuery(
                        string.Format(
                            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}' AND COLUMN_NAME='{1}'",
                            table, column)))
            {
                return reader.Read();
            }
        }

        protected override void DoRenameColumn(string tableName, string oldColumnName, string newColumnName)
        {
            Column column = GetColumn(tableName, oldColumnName);

            AddColumn(tableName, new Column(newColumnName, column.Type, column.ColumnProperty, column.DefaultValue));
            ExecuteNonQuery(string.Format("UPDATE {0} SET {1}={2}", tableName, newColumnName, oldColumnName));
            RemoveColumn(tableName, oldColumnName);
        }

        public override void WipeDatabase(string databaseName)
        {
            SqlCeConnection connection = (SqlCeConnection) Connection;

            if (connection.State != ConnectionState.Closed)
            {
                connection.Close();
            }

            if (File.Exists(connection.DataSource))
            {
                File.Delete(connection.DataSource);
            }

            CreateDatabaseFile();
            connection.Open();
        }


        private void CreateDatabaseFile()
        {
            using (SqlCeEngine engine = new SqlCeEngine(Connection.ConnectionString))
            {
                engine.CreateDatabase();
            }
        }


        protected override string FindConstraints(string table, string column)
        {
            return
                string.Format("SELECT cont.constraint_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE cont "
                              + "WHERE cont.Table_Name='{0}' AND cont.column_name = '{1}'",
                              table, column);
        }
    }
}