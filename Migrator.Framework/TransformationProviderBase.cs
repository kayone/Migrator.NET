using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Migrator.Framework.Exceptions;
using NLog;

namespace Migrator.Framework
{
    /// <summary>
    ///   Base class for every transformation providers. A 'transformation' is an operation that modifies the database.
    /// </summary>
    public abstract class TransformationProviderBase
    {
        protected readonly string ConnectionString;

        private readonly ForeignKeyConstraintMapper _constraintMapper = new ForeignKeyConstraintMapper();
        private List<long> _appliedMigrations;


        private IDbTransaction _transaction;

        protected TransformationProviderBase(string connectionString)
        {
            ConnectionString = connectionString;
            Logger = LogManager.GetCurrentClassLogger();
        }

        public IDbConnection Connection { get; protected set; }

        public abstract Dialect Dialect { get; }

        public Logger Logger { get; private set; }

        /// <summary>
        ///   The list of Migrations currently applied to the database.
        /// </summary>
        public List<long> AppliedMigrations
        {
            get
            {
                if (_appliedMigrations == null)
                {
                    _appliedMigrations = new List<long>();
                    CreateSchemaInfoTable();

                    string versionColumn = "Version";

                    versionColumn = QuoteColumnNameIfRequired(versionColumn);

                    using (IDataReader reader = Select(versionColumn, "SchemaInfo"))
                    {
                        while (reader.Read())
                        {
                            if (reader.GetFieldType(0) == typeof (Decimal))
                            {
                                _appliedMigrations.Add((long) reader.GetDecimal(0));
                            }
                            else
                            {
                                _appliedMigrations.Add(reader.GetInt64(0));
                            }
                        }
                    }
                }
                return _appliedMigrations;
            }
        }

        #region Columns

        protected virtual void ChangeColumn(string table, string sqlColumn)
        {
            table = QuoteTableNameIfRequired(table);
            ExecuteNonQuery(String.Format("ALTER TABLE {0} ALTER COLUMN {1}", table, sqlColumn));
        }

        protected virtual void AddColumn(string table, string sqlColumn)
        {
            table = QuoteTableNameIfRequired(table);
            ExecuteNonQuery(String.Format("ALTER TABLE {0} ADD COLUMN {1}", table, sqlColumn));
        }

        public void RenameColumn(string tableName, string oldColumnName, string newColumnName)
        {
            if (ColumnExists(tableName, newColumnName))
                throw new ColumnAlreadyExistsException(tableName, newColumnName);

            if (!ColumnExists(tableName, oldColumnName))
                throw new ColumnDoesntExistsException(tableName, newColumnName);

            DoRenameColumn(tableName, oldColumnName, newColumnName);
        }

        protected virtual void DoRenameColumn(string tableName, string oldColumnName, string newColumnName)
        {
            ExecuteNonQuery("ALTER TABLE {0} RENAME COLUMN {1} TO {2}", tableName, oldColumnName, newColumnName);
        }

        public void RemoveColumn(string table, string column)
        {
            if (!TableExists(table))
                throw new TableDoesntExistsException(table);
            if (!ColumnExists(table, column))
                throw new ColumnDoesntExistsException(table, column);

            DoRemoveColumn(table, column);
        }

        protected virtual void DoRemoveColumn(string table, string column)
        {
            column = QuoteColumnNameIfRequired(column);
            ExecuteNonQuery("ALTER TABLE {0} DROP COLUMN {1} ", table, column);
        }

        public virtual bool ColumnExists(string table, string column)
        {
            return GetColumns(table).Any(c => string.Equals(c.Name, column, StringComparison.CurrentCultureIgnoreCase));
        }

        public virtual void ChangeColumn(string table, Column column)
        {
            if (!ColumnExists(table, column.Name))
            {
                Logger.Warn("Column {0}.{1} does not exist", table, column.Name);
                return;
            }

            ColumnPropertiesMapper mapper = Dialect.GetAndMapColumnProperties(column);

            ChangeColumn(table, mapper.ColumnSql);
        }

        public Column GetColumn(string table, string columnName)
        {
            return Array.Find(GetColumns(table), column => column.Name == columnName);
        }

        public virtual Column[] GetColumns(string table)
        {
            var columns = new List<Column>();
            using (
                IDataReader reader =
                    ExecuteQuery(
                        "select COLUMN_NAME, IS_NULLABLE from INFORMATION_SCHEMA.COLUMNS where table_name = '{0}'",
                        table))
            {
                while (reader.Read())
                {
                    Column column = new Column(reader.GetString(0), DbType.String);
                    string nullableStr = reader.GetString(1);
                    bool isNullable = nullableStr == "YES";
                    column.ColumnProperty |= isNullable ? ColumnProperty.Null : ColumnProperty.NotNull;

                    columns.Add(column);
                }
            }

            return columns.ToArray();
        }

        #endregion

        #region Tables

        public virtual bool TableExists(string table)
        {
            table = table.Trim('[', ']');

            return GetTables().Any(c => String.Equals(c, table, StringComparison.CurrentCultureIgnoreCase));
        }

        public void AddTable(string name, params Column[] columns)
        {
            if (TableExists(name)) throw new TableAlreadyExistsException(name);

            if (GetPrimaryKeys(columns).Count() > 1)
                new InvalidOperationException("Compound primary keys aren't supported");


            var columnProviders = new List<ColumnPropertiesMapper>(columns.Length);
            foreach (var column in columns)
            {
                ColumnPropertiesMapper mapper = Dialect.GetAndMapColumnProperties(column);
                columnProviders.Add(mapper);
            }

            string columnsAndIndexes = JoinColumnsAndIndexes(columnProviders);
            AddTable(name, columnsAndIndexes);
        }

        public void DropTable(string name)
        {
            if (TableExists(name))
                ExecuteNonQuery("DROP TABLE {0}", name);
        }

        public virtual void RenameTable(string oldName, string newName)
        {
            if (TableExists(newName))
                throw new TableAlreadyExistsException(newName);

            if (!TableExists(oldName))
                throw new TableDoesntExistsException(oldName);

            if (TableExists(oldName))
                ExecuteNonQuery(String.Format("ALTER TABLE {0} RENAME TO {1}", oldName, newName));
        }

        public virtual IEnumerable<string> GetTables()
        {
            var tables = new List<string>();
            using (IDataReader reader = ExecuteQuery("SELECT table_name FROM information_schema.tables"))
            {
                while (reader.Read())
                {
                    tables.Add((string) reader[0]);
                }
            }
            return tables.ToArray();
        }

        #endregion

        #region ForeignKey

        public virtual void RemoveForeignKey(string table, string name)
        {
            RemoveConstraint(table, name);
        }

        #endregion

        #region Constraint

        public string AddPrimaryKey(string tableName, string columnName)
        {
            string name = string.Format("PK_{0}_{1}", tableName.ToUpper(), columnName.ToUpper());

            if (ConstraintExists(tableName, name))
                new ConstraintAlreadyExistsException(tableName, name);

            ExecuteNonQuery("ALTER TABLE {0} ADD CONSTRAINT {1} PRIMARY KEY ({2}) ", tableName, name, columnName);

            return name;
        }

        public string AddUniqueConstraint(string tableName, params string[] columns)
        {
            string name = GetKeyName("UQ", tableName, columns);

            if (ConstraintExists(tableName, name))
                new ConstraintAlreadyExistsException(tableName, name);

            QuoteColumnNames(columns);

            tableName = QuoteTableNameIfRequired(tableName);

            ExecuteNonQuery("ALTER TABLE {0} ADD CONSTRAINT {1} UNIQUE({2}) ", tableName, name,
                            string.Join(", ", columns));

            return name;
        }

        public virtual string AddCheckConstraint(string name, string tableName, string checkSql)
        {
            if (ConstraintExists(tableName, name))
                new ConstraintAlreadyExistsException(tableName, name);

            tableName = QuoteTableNameIfRequired(tableName);

            ExecuteNonQuery(String.Format("ALTER TABLE {0} ADD CONSTRAINT {1} CHECK ({2}) ", tableName, name, checkSql));


            return name;
        }

        public virtual ForeignKey AddForeignKey(ForeignKey foreignKey)
        {
            if (ConstraintExists(foreignKey.ForeignTable, foreignKey.Name))
                throw new ForeignKeyAlreadyExistsException(foreignKey.ForeignTable, foreignKey.Name);

            foreignKey.PrimaryTable = QuoteTableNameIfRequired(foreignKey.PrimaryTable);
            foreignKey.ForeignTable = QuoteTableNameIfRequired(foreignKey.ForeignTable);
            QuoteColumnNames(foreignKey.ForeignColumns);
            QuoteColumnNames(foreignKey.PrimaryColumns);

            string constraintResolved = _constraintMapper.SqlForConstraint(foreignKey.ConstraintType);

            ExecuteNonQuery(
                String.Format(
                    "ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON UPDATE {5} ON DELETE {6}",
                    foreignKey.ForeignTable, foreignKey.Name, String.Join(",", foreignKey.ForeignColumns),
                    foreignKey.PrimaryTable, String.Join(",", foreignKey.PrimaryColumns), constraintResolved,
                    constraintResolved));


            return foreignKey;
        }


        public IEnumerable<string> GetConstraints(string tableName)
        {
            if (!TableExists(tableName))
                throw new TableDoesntExistsException(tableName);

            return
                ExecuteStringQuery(
                    "SELECT CONSTRAINT_NAME FROM information_schema.table_constraints where table_name = '{0}'",
                    tableName);
        }

        public bool ConstraintExists(string tableName, string constraintName)
        {
            return GetConstraints(tableName).Any(
                c => string.Equals(c, constraintName, StringComparison.InvariantCultureIgnoreCase));
        }


        public virtual bool PrimaryKeyExists(string table, string name)
        {
            return ConstraintExists(table, name);
        }

        public virtual void RemoveConstraint(string table, string name)
        {
            if (TableExists(table) && ConstraintExists(table, name))
            {
                table = Dialect.TableNameNeedsQuote ? Dialect.Quote(table) : table;
                name = Dialect.ConstraintNameNeedsQuote ? Dialect.Quote(name) : name;
                ExecuteNonQuery("ALTER TABLE {0} DROP CONSTRAINT {1}", table, name);
            }
        }

        #endregion

        #region Databases

        public virtual void WipeDatabase(string databaseName)
        {
            Connection.ChangeDatabase(databaseName);

            var tables = GetTables().ToList();

            foreach (var table in tables)
            {
                DropTable(table);
            }
        }

        public void SwitchDatabase(string databaseName)
        {
            Connection.ChangeDatabase(databaseName);
        }

        public bool DatabaseExists(string name)
        {
            return GetDatabases().Any(c => string.Equals(name, c, StringComparison.InvariantCultureIgnoreCase));
        }

        public void CreateDatabases(string databaseName)
        {
            ExecuteNonQuery("CREATE DATABASE {0}", databaseName);
        }

        public void DropDatabases(string databaseName)
        {
            ExecuteNonQuery("DROP DATABASE {0}", databaseName);
        }

        public abstract List<string> GetDatabases();

        #endregion

        #region Indexes

        public string AddIndex(string table, params string[] columns)
        {
            string indexName = string.Format("IDX_{0}_{1}", table, string.Join("_ ", columns));

            if (IndexExists(indexName, table))
                throw new IndexAlreadyExistsException(table, indexName);

            ExecuteNonQuery(String.Format("CREATE INDEX {0} ON {1} ({2}) ", indexName, table, string.Join(", ", columns)));

            return indexName;
        }

        public List<string> GetIndexes(string tableName)
        {
            var indexes =
                ExecuteStringQuery("SELECT Index_Name FROM INFORMATION_SCHEMA.Indexes WHERE Table_Name = '{0}'",
                                   tableName);
            return indexes;
        }

        public bool IndexExists(string indexName, string tableName)
        {
            return
                GetIndexes(tableName).Any(c => string.Equals(c, indexName, StringComparison.InvariantCultureIgnoreCase));
        }

        #endregion

        #region Execute

        public virtual int Update(string table, string[] columns, string[] values, string where = null)
        {
            string namesAndValues = JoinColumnsAndValues(columns, values);

            string query = "UPDATE {0} SET {1}";
            if (!String.IsNullOrEmpty(@where))
            {
                query += " WHERE " + @where;
            }

            return ExecuteNonQuery(String.Format(query, table, namesAndValues));
        }

        public virtual int Insert(string table, string[] columns, string[] values)
        {
            if (string.IsNullOrEmpty(table)) throw new ArgumentNullException("table");
            if (columns == null) throw new ArgumentNullException("columns");
            if (values == null) throw new ArgumentNullException("values");
            if (columns.Length != values.Length)
                throw new Exception(
                    string.Format("The number of columns: {0} does not match the number of supplied values: {1}",
                                  columns.Length, values.Length));

            table = QuoteTableNameIfRequired(table);

            string columnNames = string.Join(", ", columns.Select(QuoteColumnNameIfRequired).ToArray());

            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < values.Length; i++)
            {
                if (builder.Length > 0) builder.Append(", ");
                builder.Append(GenerateParameterName(i));
            }

            string parameterNames = builder.ToString();

            using (IDbCommand command = Connection.CreateCommand())
            {
                EnsureHasConnection();


                command.Transaction = _transaction;

                command.CommandText = String.Format("INSERT INTO {0} ({1}) VALUES ({2})", table, columnNames,
                                                    parameterNames);
                command.CommandType = CommandType.Text;


                Logger.Trace(command.CommandText);

                int paramCount = 0;

                foreach (var value in values)
                {
                    IDbDataParameter parameter = command.CreateParameter();

                    ConfigureParameterWithValue(parameter, paramCount, value);

                    parameter.ParameterName = GenerateParameterName(paramCount);

                    command.Parameters.Add(parameter);

                    paramCount++;
                }

                return command.ExecuteNonQuery();
            }
        }

        public int DeleteData(string table, string[] columns = null, string[] values = (string[]) null)
        {
            if (null == columns || null == values)
            {
                return ExecuteNonQuery("DELETE FROM {0}", table);
            }
            return
                ExecuteNonQuery("DELETE FROM {0} WHERE ({1})", table, JoinColumnsAndValues(columns, values));
        }

        public int DeleteData(string table, string wherecolumn, string wherevalue)
        {
            if (string.IsNullOrEmpty(wherecolumn) && string.IsNullOrEmpty(wherevalue))
            {
                return DeleteData(table);
            }

            return ExecuteNonQuery("DELETE FROM {0} WHERE {1} = {2}", table, wherecolumn, QuoteValues(wherevalue));
        }

        public IDataReader Select(string what, string from, string where = "1=1")
        {
            return ExecuteQuery("SELECT {0} FROM {1} WHERE {2}", what, @from, @where);
        }

        public object SelectScalar(string what, string from, string where = "1=1")
        {
            return ExecuteScalar("SELECT {0} FROM {1} WHERE {2}", what, @from, @where);
        }

        /// <summary>
        ///   Execute an SQL query returning results.
        /// </summary>
        /// <param name="sql"> The SQL command. </param>
        /// <param name="args"> </param>
        /// <returns> A data iterator, <see cref="System.Data.IDataReader">IDataReader</see> . </returns>
        public IDataReader ExecuteQuery(string sql, params object[] args)
        {
            sql = string.Format(sql, args);

            Logger.Trace(sql);
            using (IDbCommand cmd = BuildCommand(sql))
            {
                return cmd.ExecuteReader();
            }
        }

        public List<string> ExecuteStringQuery(string sql, params object[] args)
        {
            var values = new List<string>();

            using (IDataReader reader = ExecuteQuery(sql, args))
            {
                while (reader.Read())
                {
                    object value = reader[0];

                    if (value == null || value == DBNull.Value)
                    {
                        values.Add(null);
                    }
                    else
                    {
                        values.Add(value.ToString());
                    }
                }
            }

            return values;
        }

        public virtual object ExecuteScalar(string sql, params object[] args)
        {
            sql = string.Format(sql, args);

            Logger.Trace(sql);
            using (IDbCommand cmd = BuildCommand(sql))
            {
                try
                {
                    return cmd.ExecuteScalar();
                }
                catch
                {
                    Logger.Warn("Query failed: {0}", cmd.CommandText);
                    throw;
                }
            }
        }

        public virtual int ExecuteNonQuery(string sql, params object[] args)
        {
            sql = string.Format(sql, args);

            Logger.Trace(sql);

            using (IDbCommand cmd = BuildCommand(sql))
            {
                return cmd.ExecuteNonQuery();
            }
        }

        #endregion

        #region Transaction

        /// <summary>
        ///   Starts a transaction. Called by the migration mediator.
        /// </summary>
        public virtual void BeginTransaction()
        {
            if (_transaction == null && Connection != null)
            {
                EnsureHasConnection();
                _transaction = Connection.BeginTransaction(IsolationLevel.ReadCommitted);
            }
        }

        /// <summary>
        ///   Rollback the current migration. Called by the migration mediator.
        /// </summary>
        public virtual void Rollback()
        {
            if (_transaction != null && Connection != null && Connection.State == ConnectionState.Open)
            {
                try
                {
                    _transaction.Rollback();
                }
                finally
                {
                    Connection.Close();
                }
            }
            _transaction = null;
        }

        /// <summary>
        ///   Commit the current transaction. Called by the migrations mediator.
        /// </summary>
        public virtual void Commit()
        {
            if (_transaction != null && Connection != null && Connection.State == ConnectionState.Open)
            {
                try
                {
                    _transaction.Commit();
                }
                finally
                {
                    Connection.Close();
                }
            }
            _transaction = null;
        }

        #endregion

        /// <summary>
        ///   Marks a Migration version number as having been applied
        /// </summary>
        /// <param name="version"> The version number of the migration that was applied </param>
        public void MigrationApplied(long version)
        {
            CreateSchemaInfoTable();
            Insert("SchemaInfo", new[] {"Version"}, new[] {version.ToString()});
            _appliedMigrations.Add(version);
        }

        /// <summary>
        ///   Marks a Migration version number as having been rolled back from the database
        /// </summary>
        /// <param name="version"> The version number of the migration that was removed </param>
        public void MigrationUnApplied(long version)
        {
            CreateSchemaInfoTable();
            DeleteData("SchemaInfo", "Version", version.ToString());
            _appliedMigrations.Remove(version);
        }

        public virtual void AddColumn(string table, Column column)
        {
            if (!TableExists(table))
                throw new TableDoesntExistsException(table);

            if (ColumnExists(table, column.Name))
                throw new ColumnAlreadyExistsException(table, column.Name);

            ColumnPropertiesMapper mapper = Dialect.GetAndMapColumnProperties(column);

            AddColumn(table, mapper.ColumnSql);
        }


        public void Dispose()
        {
            if (Connection != null && Connection.State != ConnectionState.Closed)
            {
                Connection.Close();
                Connection.Dispose();
            }
        }

        private string QuoteColumnNameIfRequired(string name)
        {
            if (Dialect.ColumnNameNeedsQuote || Dialect.IsReservedWord(name))
            {
                return Dialect.Quote(name);
            }
            return name;
        }

        private string QuoteTableNameIfRequired(string name)
        {
            if (Dialect.TableNameNeedsQuote || Dialect.IsReservedWord(name))
            {
                return Dialect.Quote(name);
            }
            return name;
        }

        protected void AddTable(string table, string columns)
        {
            table = Dialect.TableNameNeedsQuote ? Dialect.Quote(table) : table;
            string sqlCreate = String.Format("CREATE TABLE {0} ({1})", table, columns);
            ExecuteNonQuery(sqlCreate);
        }

        private IEnumerable<Column> GetPrimaryKeys(IEnumerable<Column> columns)
        {
            return columns.Where(c => c.IsPrimaryKey);
        }

        private string JoinColumnsAndIndexes(IEnumerable<ColumnPropertiesMapper> columns)
        {
            string indexes = JoinIndexes(columns);
            string columnsAndIndexes = JoinColumns(columns) + (indexes != null ? "," + indexes : String.Empty);
            return columnsAndIndexes;
        }

        private string JoinIndexes(IEnumerable<ColumnPropertiesMapper> columns)
        {
            var indexes = columns.Select(column => column.IndexSql).Where(indexSql => indexSql != null).ToList();

            if (!indexes.Any())
                return null;

            return String.Join(", ", indexes);
        }

        private static string JoinColumns(IEnumerable<ColumnPropertiesMapper> columns)
        {
            return String.Join(", ", columns.Select(column => column.ColumnSql));
        }

        private IDbCommand BuildCommand(string sql)
        {
            EnsureHasConnection();
            IDbCommand cmd = Connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandType = CommandType.Text;
            if (_transaction != null)
            {
                cmd.Transaction = _transaction;
            }

            return cmd;
        }

        private void EnsureHasConnection()
        {
            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }
        }

        private void CreateSchemaInfoTable()
        {
            EnsureHasConnection();
            if (!TableExists("SchemaInfo"))
            {
                AddTable("SchemaInfo", new Column("Version", DbType.Int64, ColumnProperty.PrimaryKey));
            }
        }

        private string QuoteValues(string values)
        {
            return QuoteValues(new[] {values})[0];
        }

        private string[] QuoteValues(string[] values)
        {
            return Array.ConvertAll(values,
                                    delegate(string val)
                                        {
                                            if (null == val)
                                                return "null";
                                            else
                                                return String.Format("'{0}'", val.Replace("'", "''"));
                                        });
        }

        private string JoinColumnsAndValues(string[] columns, string[] values)
        {
            var quotedValues = QuoteValues(values);
            var namesAndValues = new string[columns.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                namesAndValues[i] = String.Format("{0}={1}", columns[i], quotedValues[i]);
            }

            return String.Join(", ", namesAndValues);
        }

        private string GenerateParameterName(int index)
        {
            return "@p" + index;
        }

        private void ConfigureParameterWithValue(IDbDataParameter parameter, int index, object value)
        {
            if (value == null || value == DBNull.Value)
            {
                parameter.Value = DBNull.Value;
            }
            else if (value is Guid)
            {
                parameter.DbType = DbType.Guid;
                parameter.Value = (Guid) value;
            }
            else if (value is Int32)
            {
                parameter.DbType = DbType.Int32;
                parameter.Value = value;
            }
            else if (value is Int64)
            {
                parameter.DbType = DbType.Int64;
                parameter.Value = value;
            }
            else if (value is String)
            {
                parameter.DbType = DbType.String;
                parameter.Value = value;
            }
            else if (value is DateTime)
            {
                parameter.DbType = DbType.DateTime;
                parameter.Value = value;
            }
            else if (value is Boolean)
            {
                parameter.DbType = DbType.Boolean;
                parameter.Value = value;
            }
            else
            {
                throw new NotSupportedException(
                    string.Format("TransformationProvider does not support value: {0} of type: {1}", value,
                                  value.GetType()));
            }
        }

        private void QuoteColumnNames(string[] primaryColumns)
        {
            for (int i = 0; i < primaryColumns.Length; i++)
            {
                primaryColumns[i] = QuoteColumnNameIfRequired(primaryColumns[i]);
            }
        }


        private static string GetKeyName(string prefix, string tableName, string[] Columns)
        {
            var cols = Columns.Select(c => c.ToUpper());
            return string.Format("{0}_{1}_{2}", prefix, tableName, string.Join("_", cols));
        }
    }
}