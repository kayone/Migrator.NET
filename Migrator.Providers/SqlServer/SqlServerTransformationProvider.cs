#region License

//The contents of this file are subject to the Mozilla Public License
//Version 1.1 (the "License"); you may not use this file except in
//compliance with the License. You may obtain a copy of the License at
//http://www.mozilla.org/MPL/
//Software distributed under the License is distributed on an "AS IS"
//basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
//License for the specific language governing rights and limitations
//under the License.

#endregion

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Migrator.Framework;
using Migrator.Framework.Exceptions;

namespace Migrator.Providers.SqlServer
{
    /// <summary>
    /// Migration transformations provider for Microsoft SQL Server.
    /// </summary>
    public class SqlServerTransformationProvider : TransformationProviderBase
    {
        public const string DefaultSchema = "dbo";

        public SqlServerTransformationProvider(string connectionString)
            : base(connectionString)
        {
            CreateConnection();
        }

        protected virtual void CreateConnection()
        {
            Connection = new SqlConnection(_connectionString);
            Connection.Open();
        }

        // FIXME: We should look into implementing this with INFORMATION_SCHEMA if possible
        // so that it would be usable by all the SQL Server implementations
        public override bool ConstraintExists(string table, string name)
        {
            using (IDataReader reader = ExecuteQuery("SELECT TOP 1 * FROM sysobjects WHERE id = object_id('{0}')", name))
            {
                return reader.Read();
            }
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

        public override Dialect Dialect
        {
            get { return new SqlServerDialect(); }
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
        void DeleteColumnConstraints(string table, string column)
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
            foreach (string constraint in constraints)
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

        public virtual void RemoveIndex(string table, string name)
        {
            throw new NotImplementedException();
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