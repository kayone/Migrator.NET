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
using System.Linq;
using System.Text;
using Migrator.Framework;
using Migrator.Framework.Exceptions;
using Migrator.Framework.SchemaBuilder;
using NLog;

namespace Migrator.Providers
{
    /// <summary>
    /// Base class for every transformation providers.
    /// A 'transformation' is an operation that modifies the database.
    /// </summary>
    public abstract class TransformationProviderBase : ITransformationProvider
    {
        protected readonly string _connectionString;

        readonly ForeignKeyConstraintMapper _constraintMapper = new ForeignKeyConstraintMapper();
        List<long> _appliedMigrations;
        public IDbConnection Connection { get; set; }


        IDbTransaction _transaction;

        public TransformationProviderBase(string connectionString)
        {

            _connectionString = connectionString;
            Logger = LogManager.GetCurrentClassLogger();
        }

        public abstract Dialect Dialect { get; }

        public string ConnectionString { get { return _connectionString; } }

        public Logger Logger { get; private set; }


        public virtual Column[] GetColumns(string table)
        {
            var columns = new List<Column>();
            using (IDataReader reader = ExecuteQuery("select COLUMN_NAME, IS_NULLABLE from INFORMATION_SCHEMA.COLUMNS where table_name = '{0}'", table))
            {
                while (reader.Read())
                {
                    var column = new Column(reader.GetString(0), DbType.String);
                    string nullableStr = reader.GetString(1);
                    bool isNullable = nullableStr == "YES";
                    column.ColumnProperty |= isNullable ? ColumnProperty.Null : ColumnProperty.NotNull;

                    columns.Add(column);
                }
            }

            return columns.ToArray();
        }

        public virtual Column GetColumnByName(string table, string columnName)
        {
            return Array.Find(GetColumns(table), column => column.Name == columnName);
        }

        public virtual string[] GetTables()
        {
            var tables = new List<string>();
            using (IDataReader reader = ExecuteQuery("SELECT table_name FROM information_schema.tables"))
            {
                while (reader.Read())
                {
                    tables.Add((string)reader[0]);
                }
            }
            return tables.ToArray();
        }

        public virtual void RemoveForeignKey(string table, string name)
        {
            RemoveConstraint(table, name);
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


        public virtual void AddTable(string name, params Column[] columns)
        {
            AddTable(name, null, columns);
        }

        public virtual void AddTable(string name, string engine, params Column[] columns)
        {
            if (TableExists(name)) throw new TableAlreadyExistsException(name);

            if (GetPrimaryKeys(columns).Count() > 1)
                new InvalidOperationException("Compound primary keys aren't supported");


            var columnProviders = new List<ColumnPropertiesMapper>(columns.Length);
            foreach (Column column in columns)
            {
                ColumnPropertiesMapper mapper = Dialect.GetAndMapColumnProperties(column);
                columnProviders.Add(mapper);
            }

            string columnsAndIndexes = JoinColumnsAndIndexes(columnProviders);
            AddTable(name, engine, columnsAndIndexes);
        }

        public virtual void DeleteTable(string name)
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

        public virtual bool TableExists(string table)
        {
            table = table.Trim('[', ']');

            return GetTables().Any(c => String.Equals(c, table, StringComparison.CurrentCultureIgnoreCase));
        }

        public void SwitchDatabase(string databaseName)
        {
            Connection.ChangeDatabase(databaseName);
        }

        public abstract List<string> GetDatabases();

        public bool DatabaseExists(string name)
        {
            return GetDatabases().Any(c => string.Equals(name, c, StringComparison.InvariantCultureIgnoreCase));
        }

        public virtual void CreateDatabases(string databaseName)
        {
            ExecuteNonQuery("CREATE DATABASE {0}", databaseName);
        }

        public virtual void DropDatabases(string databaseName)
        {
            ExecuteNonQuery("DROP DATABASE {0}", databaseName);
        }

        public virtual bool IndexExists(string indexName, string tableName)
        {
            try
            {
                var count = (int)ExecuteScalar(String.Format("SELECT COUNT(*) FROM INFORMATION_SCHEMA.Indexes WHERE Index_Name = '{0}' AND Table_Name = '{1}'  ", indexName, tableName));
                return count == 1;
            }
            catch (Exception)
            {
                return false;
            }
        }


        public virtual void AddIndex(string name, string table, params string[] columns)
        {
            if (IndexExists(name, table))
            {
                Logger.Warn("Index {0} already exists", name);
                return;
            }
            ExecuteNonQuery(String.Format("CREATE INDEX {0} ON {1} ({2}) ", name, table, string.Join(", ", columns)));
        }

        public virtual void WipeDatabase(string databaseName)
        {
            Connection.ChangeDatabase(databaseName);

            var tables = GetTables();

            foreach (var table in tables)
            {
                DeleteTable(table);
            }
        }

        public virtual void AddColumn(string table, string column, DbType type, int size, ColumnProperty property,
                                      object defaultValue)
        {
            if (!TableExists(table))
                throw new TableDoesntExistsException(table);

            if (ColumnExists(table, column))
                throw new ColumnAlreadyExistsException(table, column);

            ColumnPropertiesMapper mapper = Dialect.GetAndMapColumnProperties(new Column(column, type, size, property, defaultValue));

            AddColumn(table, mapper.ColumnSql);
        }


        public virtual void AddColumn(string table, string column, DbType type)
        {
            AddColumn(table, column, type, 0, ColumnProperty.Null, null);
        }

        public virtual void AddColumn(string table, string column, DbType type, int size)
        {
            AddColumn(table, column, type, size, ColumnProperty.Null, null);
        }

        public virtual void AddColumn(string table, string column, DbType type, object defaultValue)
        {
            if (ColumnExists(table, column))
            {
                Logger.Warn("Column {0}.{1} already exists", table, column);
                return;
            }

            ColumnPropertiesMapper mapper = Dialect.GetAndMapColumnProperties(new Column(column, type, defaultValue));

            AddColumn(table, mapper.ColumnSql);
        }

        public virtual void AddColumn(string table, string column, DbType type, ColumnProperty property)
        {
            AddColumn(table, column, type, 0, property, null);
        }

        public virtual void AddColumn(string table, string column, DbType type, int size, ColumnProperty property)
        {
            AddColumn(table, column, type, size, property, null);
        }

        public void AddPrimaryKey(string tableName, string columnName)
        {
            var name = string.Format("PK_{0}_{1}", tableName.ToUpper(), columnName.ToUpper());

            if (ConstraintExists(tableName, name))
            {
                throw new InvalidOperationException();
            }

            ExecuteNonQuery("ALTER TABLE {0} ADD CONSTRAINT {1} PRIMARY KEY ({2}) ", tableName, name, columnName);
        }

        public virtual void AddUniqueConstraint(string name, string table, params string[] columns)
        {
            if (ConstraintExists(table, name))
            {
                Logger.Warn("Constraint {0} already exists", name);
                return;
            }

            QuoteColumnNames(columns);

            table = QuoteTableNameIfRequired(table);

            ExecuteNonQuery(String.Format("ALTER TABLE {0} ADD CONSTRAINT {1} UNIQUE({2}) ", table, name, string.Join(", ", columns)));
        }

        public virtual void AddCheckConstraint(string name, string table, string checkSql)
        {
            if (ConstraintExists(table, name))
            {
                Logger.Warn("Constraint {0} already exists", name);
                return;
            }

            table = QuoteTableNameIfRequired(table);

            ExecuteNonQuery(String.Format("ALTER TABLE {0} ADD CONSTRAINT {1} CHECK ({2}) ", table, name, checkSql));
        }

        public virtual void AddForeignKey(string primaryTable, string primaryColumn, string refTable, string refColumn)
        {
            AddForeignKey("FK_" + primaryTable + "_" + refTable, primaryTable, primaryColumn, refTable, refColumn);
        }

        public virtual void AddForeignKey(string primaryTable, string[] primaryColumns, string refTable,
                                               string[] refColumns)
        {
            AddForeignKey("FK_" + primaryTable + "_" + refTable, primaryTable, primaryColumns, refTable, refColumns);
        }

        public virtual void AddForeignKey(string primaryTable, string primaryColumn, string refTable,
                                               string refColumn, ForeignKeyConstraintType constraintType)
        {
            AddForeignKey("FK_" + primaryTable + "_" + refTable, primaryTable, primaryColumn, refTable, refColumn,
                          constraintType);
        }

        public virtual void AddForeignKey(string primaryTable, string[] primaryColumns, string refTable,
                                               string[] refColumns, ForeignKeyConstraintType constraintType)
        {
            AddForeignKey("FK_" + primaryTable + "_" + refTable, primaryTable, primaryColumns, refTable, refColumns,
                          constraintType);
        }

        /// <summary>
        /// Append a foreign key (relation) between two tables.
        /// tables.
        /// </summary>
        /// <param name="name">Constraint name</param>
        /// <param name="primaryTable">Table name containing the primary key</param>
        /// <param name="primaryColumn">Primary key column name</param>
        /// <param name="refTable">Foreign table name</param>
        /// <param name="refColumn">Foreign column name</param>
        public virtual void AddForeignKey(string name, string primaryTable, string primaryColumn, string refTable,
                                          string refColumn)
        {
            try
            {
                AddForeignKey(name, primaryTable, new[] { primaryColumn }, refTable, new[] { refColumn });
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("Error occured while adding foreign key: \"{0}\" between table: \"{1}\" and table: \"{2}\" - see inner exception for details", name, primaryTable, refTable), ex);
            }
        }

        /// <summary>
        /// <see cref="ITransformationProvider.AddForeignKey(string, string, string, string, string)">
        /// AddForeignKey(string, string, string, string, string)
        /// </see>
        /// </summary>
        public virtual void AddForeignKey(string name, string primaryTable, string[] primaryColumns, string refTable, string[] refColumns)
        {
            AddForeignKey(name, primaryTable, primaryColumns, refTable, refColumns, ForeignKeyConstraintType.NoAction);
        }

        public virtual void AddForeignKey(string name, string primaryTable, string primaryColumn, string refTable, string refColumn, ForeignKeyConstraintType constraintType)
        {
            AddForeignKey(name, primaryTable, new[] { primaryColumn }, refTable, new[] { refColumn },
                          constraintType);
        }

        public virtual void AddForeignKey(string name, string primaryTable, string[] primaryColumns, string refTable,
                                          string[] refColumns, ForeignKeyConstraintType constraintType)
        {
            if (ConstraintExists(primaryTable, name))
            {
                Logger.Warn("Constraint {0} already exists", name);
                return;
            }

            refTable = QuoteTableNameIfRequired(refTable);
            primaryTable = QuoteTableNameIfRequired(primaryTable);
            QuoteColumnNames(primaryColumns);
            QuoteColumnNames(refColumns);

            string constraintResolved = _constraintMapper.SqlForConstraint(constraintType);

            ExecuteNonQuery(
                String.Format(
                    "ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON UPDATE {5} ON DELETE {6}",
                    primaryTable, name, String.Join(",", primaryColumns),
                    refTable, String.Join(",", refColumns), constraintResolved, constraintResolved));
        }

        /// <summary>
        /// Determines if a constraint exists.
        /// </summary>
        /// <param name="name">Constraint name</param>
        /// <param name="table">Table owning the constraint</param>
        /// <returns><c>true</c> if the constraint exists.</returns>
        public abstract bool ConstraintExists(string table, string name);

        public virtual bool PrimaryKeyExists(string table, string name)
        {
            return ConstraintExists(table, name);
        }

        public virtual int ExecuteNonQuery(string sql, params object[] args)
        {
            sql = string.Format(sql, args);

            Logger.Trace(sql);

            using (IDbCommand cmd = BuildCommand(sql))
            {
                try
                {
                    return cmd.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Logger.Warn(ex.Message);
                    throw;
                }
            }
        }

        /// <summary>
        /// Execute an SQL query returning results.
        /// </summary>
        /// <param name="sql">The SQL command.</param>
        /// <param name="args"> </param>
        /// <returns>A data iterator, <see cref="System.Data.IDataReader">IDataReader</see>.</returns>
        public virtual IDataReader ExecuteQuery(string sql, params object[] args)
        {
            sql = string.Format(sql, args);

            Logger.Trace(sql);
            using (IDbCommand cmd = BuildCommand(sql))
            {
                try
                {
                    return cmd.ExecuteReader();
                }
                catch (Exception ex)
                {
                    Logger.Warn("query failed: {0}", cmd.CommandText);
                    throw new Exception("Failed to execute sql statement: " + sql, ex);
                }
            }
        }

        public List<string> ExecuteStringQuery(string sql, params object[] args)
        {
            var values = new List<string>();

            using (var reader = ExecuteQuery(sql, args))
            {
                while (reader.Read())
                {
                    var value = reader[0];

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

        public virtual IDataReader Select(string what, string from)
        {
            return Select(what, from, "1=1");
        }

        public virtual IDataReader Select(string what, string from, string where)
        {
            return ExecuteQuery("SELECT {0} FROM {1} WHERE {2}", what, from, where);
        }

        public object SelectScalar(string what, string from)
        {
            return SelectScalar(what, from, "1=1");
        }

        public virtual object SelectScalar(string what, string from, string where)
        {
            return ExecuteScalar("SELECT {0} FROM {1} WHERE {2}", what, from, where);
        }

        public virtual int Update(string table, string[] columns, string[] values)
        {
            return Update(table, columns, values, null);
        }

        public virtual int Update(string table, string[] columns, string[] values, string where)
        {
            string namesAndValues = JoinColumnsAndValues(columns, values);

            string query = "UPDATE {0} SET {1}";
            if (!String.IsNullOrEmpty(where))
            {
                query += " WHERE " + where;
            }

            return ExecuteNonQuery(String.Format(query, table, namesAndValues));
        }

        public virtual int Insert(string table, string[] columns, string[] values)
        {
            if (string.IsNullOrEmpty(table)) throw new ArgumentNullException("table");
            if (columns == null) throw new ArgumentNullException("columns");
            if (values == null) throw new ArgumentNullException("values");
            if (columns.Length != values.Length) throw new Exception(string.Format("The number of columns: {0} does not match the number of supplied values: {1}", columns.Length, values.Length));

            table = QuoteTableNameIfRequired(table);

            string columnNames = string.Join(", ", columns.Select(QuoteColumnNameIfRequired).ToArray());

            var builder = new StringBuilder();

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

                command.CommandText = String.Format("INSERT INTO {0} ({1}) VALUES ({2})", table, columnNames, parameterNames);
                command.CommandType = CommandType.Text;


                Logger.Trace(command.CommandText);

                int paramCount = 0;

                foreach (string value in values)
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

        public virtual int DeleteData(string table, string[] columns, string[] values)
        {
            if (null == columns || null == values)
            {
                return ExecuteNonQuery(String.Format("DELETE FROM {0}", table));
            }
            return ExecuteNonQuery(String.Format("DELETE FROM {0} WHERE ({1})", table, JoinColumnsAndValues(columns, values)));
        }

        public virtual int DeleteData(string table, string wherecolumn, string wherevalue)
        {
            if (string.IsNullOrEmpty(wherecolumn) && string.IsNullOrEmpty(wherevalue))
            {
                return DeleteData(table, (string[])null, null);
            }

            return ExecuteNonQuery(String.Format("DELETE FROM {0} WHERE {1} = {2}", table, wherecolumn, QuoteValues(wherevalue)));
        }

        /// <summary>
        /// Starts a transaction. Called by the migration mediator.
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
        /// Rollback the current migration. Called by the migration mediator.
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
        /// Commit the current transaction. Called by the migrations mediator.
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

        /// <summary>
        /// The list of Migrations currently applied to the database.
        /// </summary>
        public virtual List<long> AppliedMigrations
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
                            if (reader.GetFieldType(0) == typeof(Decimal))
                            {
                                _appliedMigrations.Add((long)reader.GetDecimal(0));
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

        /// <summary>
        /// Marks a Migration version number as having been applied
        /// </summary>
        /// <param name="version">The version number of the migration that was applied</param>
        public void MigrationApplied(long version)
        {
            CreateSchemaInfoTable();
            Insert("SchemaInfo", new string[] { "Version" }, new[] { version.ToString() });
            _appliedMigrations.Add(version);
        }

        /// <summary>
        /// Marks a Migration version number as having been rolled back from the database
        /// </summary>
        /// <param name="version">The version number of the migration that was removed</param>
        public void MigrationUnApplied(long version)
        {
            CreateSchemaInfoTable();
            DeleteData("SchemaInfo", "Version", version.ToString());
            _appliedMigrations.Remove(version);
        }

        public virtual void AddColumn(string table, Column column)
        {
            AddColumn(table, column.Name, column.Type, column.Size, column.ColumnProperty, column.DefaultValue);
        }

        public virtual void AddForeignKey(string primaryTable, string refTable)
        {
            AddForeignKey(primaryTable, refTable, ForeignKeyConstraintType.NoAction);
        }

        public virtual void AddForeignKey(string primaryTable, string refTable, ForeignKeyConstraintType constraintType)
        {
            AddForeignKey(primaryTable, refTable + "Id", refTable, "Id", constraintType);
        }

        public virtual IDbCommand GetCommand()
        {
            return BuildCommand(null);
        }

        public virtual void ExecuteSchemaBuilder(SchemaBuilder builder)
        {
            foreach (ISchemaBuilderExpression expr in builder.Expressions)
                expr.Create(this);
        }

        public void Dispose()
        {
            if (Connection != null && Connection.State != ConnectionState.Closed)
            {
                Connection.Close();
                Connection.Dispose();
            }
        }

        public virtual string QuoteColumnNameIfRequired(string name)
        {
            if (Dialect.ColumnNameNeedsQuote || Dialect.IsReservedWord(name))
            {
                return Dialect.Quote(name);
            }
            return name;
        }

        public virtual string QuoteTableNameIfRequired(string name)
        {
            if (Dialect.TableNameNeedsQuote || Dialect.IsReservedWord(name))
            {
                return Dialect.Quote(name);
            }
            return name;
        }

        public virtual string Encode(Guid guid)
        {
            return guid.ToString();
        }

        public virtual string[] QuoteColumnNamesIfRequired(params string[] columnNames)
        {
            var quotedColumns = new string[columnNames.Length];

            for (int i = 0; i < columnNames.Length; i++)
            {
                quotedColumns[i] = QuoteColumnNameIfRequired(columnNames[i]);
            }

            return quotedColumns;
        }

        protected virtual void AddTable(string table, string engine, string columns)
        {
            table = Dialect.TableNameNeedsQuote ? Dialect.Quote(table) : table;
            string sqlCreate = String.Format("CREATE TABLE {0} ({1})", table, columns);
            ExecuteNonQuery(sqlCreate);
        }

        private IEnumerable<Column> GetPrimaryKeys(IEnumerable<Column> columns)
        {
            return columns.Where(c => c.IsPrimaryKey);
        }

        protected virtual void AddColumn(string table, string sqlColumn)
        {
            table = QuoteTableNameIfRequired(table);
            ExecuteNonQuery(String.Format("ALTER TABLE {0} ADD COLUMN {1}", table, sqlColumn));
        }

        protected virtual void ChangeColumn(string table, string sqlColumn)
        {
            table = QuoteTableNameIfRequired(table);
            ExecuteNonQuery(String.Format("ALTER TABLE {0} ALTER COLUMN {1}", table, sqlColumn));
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

        IDbCommand BuildCommand(string sql)
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

        public int DeleteData(string table)
        {
            return DeleteData(table, null, (string[])null);
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
            return QuoteValues(new[] { values })[0];
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
            string[] quotedValues = QuoteValues(values);
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
            else if (value is Guid || value is Guid?)
            {
                parameter.DbType = DbType.Guid;
                parameter.Value = (Guid)value;
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
                throw new NotSupportedException(string.Format("TransformationProvider does not support value: {0} of type: {1}", value, value.GetType()));
            }
        }

        void QuoteColumnNames(string[] primaryColumns)
        {
            for (int i = 0; i < primaryColumns.Length; i++)
            {
                primaryColumns[i] = QuoteColumnNameIfRequired(primaryColumns[i]);
            }
        }
    }
}