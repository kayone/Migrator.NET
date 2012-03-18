using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Migrator.Framework;
using Migrator.Framework.Exceptions;

namespace Migrator.Providers.SqlServer
{
    /// <summary>
    ///   Migration transformations provider for Microsoft SQL Server.
    /// </summary>
    public class SqlServerTransformationProvider : TransformationProviderBase
    {
        public const string DefaultSchema = "dbo";

        public SqlServerTransformationProvider(string connectionString)
            : base(connectionString)
        {
            CreateConnection();
        }

        public override Dialect Dialect
        {
            get { return new SqlServerDialect(); }
        }

        protected virtual void CreateConnection()
        {
            Connection = new SqlConnection(ConnectionString);
            Connection.Open();
        }


        protected override void AddColumn(string table, string sqlColumn)
        {
            ExecuteNonQuery("ALTER TABLE {0} ADD {1}", table, sqlColumn);
        }

        protected override void DoRemoveColumn(string table, string column)
        {
            DeleteColumnConstraints(table, column);
            base.DoRemoveColumn(table, column);
        }

        public override List<string> GetDatabases()
        {
            return ExecuteStringQuery("SELECT name FROM sys.databases");
        }

        protected override void DoRenameColumn(string tableName, string oldColumnName, string newColumnName)
        {
            ExecuteNonQuery("EXEC sp_rename '{0}.{1}', '{2}', 'COLUMN'", tableName, oldColumnName, newColumnName);
        }

        public override void RenameTable(string oldName, string newName)
        {
            if (TableExists(newName))
                throw new TableAlreadyExistsException(newName);

            if (!TableExists(oldName))
                throw new TableDoesntExistsException(oldName);

            ExecuteNonQuery("EXEC sp_rename '{0}', '{1}'", oldName, newName);
        }

        // Deletes all constraints linked to a column. Sql Server
        // doesn't seems to do this.
        private void DeleteColumnConstraints(string table, string column)
        {
            string sqlContrainte = FindConstraints(table, column);
            var constraints = new List<string>();
            using (IDataReader reader = ExecuteQuery(sqlContrainte))
            {
                while (reader.Read())
                {
                    constraints.Add(reader.GetString(0));
                }
            }
            // Can't share the connection so two phase modif
            foreach (var constraint in constraints)
            {
                RemoveForeignKey(table, constraint);
            }
        }

        // FIXME: We should look into implementing this with INFORMATION_SCHEMA if possible
        // so that it would be usable by all the SQL Server implementations
        protected virtual string FindConstraints(string table, string column)
        {
            return string.Format(
                "SELECT cont.name FROM sysobjects cont, syscolumns col, sysconstraints cnt  "
                + "WHERE cont.parent_obj = col.id AND cnt.constid = cont.id AND cnt.colid=col.colid "
                + "AND col.name = '{1}' AND col.id = object_id('{0}')",
                table, column);
        }

        public override bool TableExists(string table)
        {
            if (table.Contains("."))
            {
                table = table.Substring(table.IndexOf(".") + 1);
            }

            return base.TableExists(table);
        }
    }
}