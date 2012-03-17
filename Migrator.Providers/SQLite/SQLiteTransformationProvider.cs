using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using Migrator.Framework;
using Migrator.Framework.Exceptions;


namespace Migrator.Providers.SQLite
{
    /// <summary>
    /// Summary description for SQLiteTransformationProvider.
    /// </summary>
    public class SQLiteTransformationProvider : TransformationProviderBase
    {
        public SQLiteTransformationProvider(string connectionString)
            : base(connectionString)
        {
            Connection = new SQLiteConnection(ConnectionString);
            Connection.ConnectionString = ConnectionString;
            Connection.Open();
        }

        protected override void DoRemoveColumn(string table, string column)
        {
            string[] origColDefs = GetColumnDefs(table);

            string[] newColDefs = origColDefs.Where(origdef => !ColumnMatch(column, origdef)).ToArray();
            string colDefsSql = String.Join(",", newColDefs);

            string[] colNames = ParseSqlForColumnNames(newColDefs);
            string colNamesSql = String.Join(",", colNames);

            AddTable(table + "_temp", colDefsSql);
            ExecuteQuery(String.Format("INSERT INTO {0}_temp SELECT {1} FROM {0}", table, colNamesSql));
            DropTable(table);
            ExecuteQuery(String.Format("ALTER TABLE {0}_temp RENAME TO {0}", table));
        }

        protected override void DoRenameColumn(string tableName, string oldColumnName, string newColumnName)
        {
            string[] columnDefs = GetColumnDefs(tableName);
            string columnDef = Array.Find(columnDefs, col => ColumnMatch(oldColumnName, col));

            string newColumnDef = columnDef.Replace(oldColumnName, newColumnName);

            AddColumn(tableName, newColumnDef);
            ExecuteQuery(String.Format("UPDATE {0} SET {1}={2}", tableName, newColumnName, oldColumnName));
            RemoveColumn(tableName, oldColumnName);

        }

        public override void ChangeColumn(string table, Column column)
        {
            if (!ColumnExists(table, column.Name))
            {
                Logger.Warn("Column {0}.{1} does not exist", table, column.Name);
                return;
            }

            string tempColumn = "temp_" + column.Name;
            RenameColumn(table, column.Name, tempColumn);
            AddColumn(table, column);
            ExecuteQuery(String.Format("UPDATE {0} SET {1}={2}", table, column.Name, tempColumn));
            RemoveColumn(table, tempColumn);
        }

        public override List<string> GetDatabases()
        {
            throw new NotSupportedException();
        }



        public override IEnumerable<string> GetTables()
        {
            var tables = new List<string>();

            using (IDataReader reader = ExecuteQuery("SELECT name FROM sqlite_master WHERE type='table' AND name <> 'sqlite_sequence' ORDER BY name"))
            {
                while (reader.Read())
                {
                    tables.Add((string)reader[0]);
                }
            }

            return tables;
        }

        public override Dialect Dialect
        {
            get { return new SQLiteDialect(); }
        }

        public override Column[] GetColumns(string table)
        {
            var columns = new List<Column>();
            foreach (string columnDef in GetColumnDefs(table))
            {
                string name = ExtractNameFromColumnDef(columnDef);
                // FIXME: Need to get the real type information
                var column = new Column(name, DbType.String);
                bool isNullable = IsNullable(columnDef);
                column.ColumnProperty |= isNullable ? ColumnProperty.Null : ColumnProperty.NotNull;
                columns.Add(column);
            }
            return columns.ToArray();
        }

        public string GetSqlDefString(string table)
        {
            string sqldef = null;
            using (IDataReader reader = ExecuteQuery(String.Format("SELECT sql FROM sqlite_master WHERE type='table' AND name='{0}'", table)))
            {
                if (reader.Read())
                {
                    sqldef = (string)reader[0];
                }
            }
            return sqldef;
        }

        public string[] GetColumnNames(string table)
        {
            return ParseSqlForColumnNames(GetSqlDefString(table));
        }

        public string[] GetColumnDefs(string table)
        {
            return ParseSqlColumnDefs(GetSqlDefString(table));
        }

        /// <summary>
        /// Turn something like 'columnName INTEGER NOT NULL' into just 'columnName'
        /// </summary>
        public string[] ParseSqlForColumnNames(string sqldef)
        {
            string[] parts = ParseSqlColumnDefs(sqldef);
            return ParseSqlForColumnNames(parts);
        }

        public string[] ParseSqlForColumnNames(string[] parts)
        {
            if (null == parts)
                return null;

            for (int i = 0; i < parts.Length; i++)
            {
                parts[i] = ExtractNameFromColumnDef(parts[i]);
            }
            return parts;
        }

        /// <summary>
        /// Name is the first value before the space.
        /// </summary>
        /// <param name="columnDef"></param>
        /// <returns></returns>
        public string ExtractNameFromColumnDef(string columnDef)
        {
            int idx = columnDef.IndexOf(" ");
            if (idx > 0)
            {
                return columnDef.Substring(0, idx);
            }
            return null;
        }

        public bool IsNullable(string columnDef)
        {
            return !columnDef.Contains("NOT NULL");
        }

        public string[] ParseSqlColumnDefs(string sqldef)
        {
            if (String.IsNullOrEmpty(sqldef))
            {
                return null;
            }

            sqldef = sqldef.Replace(Environment.NewLine, " ");
            int start = sqldef.IndexOf("(");
            int end = sqldef.LastIndexOf(")");

            sqldef = sqldef.Substring(0, end);
            sqldef = sqldef.Substring(start + 1);

            string[] cols = sqldef.Split(new[] { ',' });
            for (int i = 0; i < cols.Length; i++)
            {
                cols[i] = cols[i].Trim();
            }
            return cols;
        }

        public bool ColumnMatch(string column, string columnDef)
        {
            return columnDef.StartsWith(column + " ") || columnDef.StartsWith(Dialect.Quote(column));
        }

        public override void WipeDatabase(string databaseName)
        {
            var tables = GetTables();

            foreach (var table in tables)
            {
                DropTable(table);
            }
        }
    }
}