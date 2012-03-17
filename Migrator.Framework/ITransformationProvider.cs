using System;
using System.Collections.Generic;
using System.Data;
using Migrator.Providers;
using NLog;

namespace Migrator.Framework
{
    /// <summary>
    /// The main interface to use in Migrations to make changes on a database schema.
    /// </summary>
    public interface ITransformationProvider : IDisposable
    {
        IDbConnection Connection { get; set; }
        Dialect Dialect { get; }

        /// <summary>
        /// The list of Migrations currently applied to the database.
        /// </summary>
        List<long> AppliedMigrations { get; }

        /// <summary>
        /// Logger used to log details of operations performed during migration
        /// </summary>
        Logger Logger { get; }

        #region Databases

        /// <summary>
        /// Get a list of databases available on the server
        /// </summary>
        List<string> GetDatabases();

        /// <summary>
        /// Checks to see if a database with specific name exists on the server
        /// </summary>
        bool DatabaseExists(string name);

        /// <summary>
        /// Change the target database
        /// </summary>
        /// <param name="databaseName">Name of the new target database</param>
        void SwitchDatabase(string databaseName);

        /// <summary>
        /// Create a new database on the server
        /// </summary>
        /// <param name="databaseName">Name of the new database</param>
        void CreateDatabases(string databaseName);

        /// <summary>
        /// Delete a database from the server
        /// </summary>
        /// <param name="databaseName">Name of the database to delete</param>
        void DropDatabases(string databaseName);

        void WipeDatabase(string databaseName);

        #endregion

        #region Tables

        /// <summary>
        /// Get the names of all of the tables
        /// </summary>
        /// <returns>The names of all the tables.</returns>
        IEnumerable<string> GetTables();

        /// <summary>
        /// Check if a table already exists
        /// </summary>
        /// <param name="tableName">The name of the table that you want to check on.</param>
        /// <returns></returns>
        bool TableExists(string tableName);

        /// <summary>
        /// Add a table
        /// </summary>
        /// <param name="name">The name of the table to add.</param>
        /// <param name="columns">The columns that are part of the table.</param>
        void AddTable(string name, params Column[] columns);


        /// <summary>
        /// Remove an existing table
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        void DropTable(string tableName);

        /// <summary>
        /// Rename an existing table
        /// </summary>
        /// <param name="oldName">The old name of the table</param>
        /// <param name="newName">The new name of the table</param>
        void RenameTable(string oldName, string newName);

        #endregion

        #region Column

        /// <summary>
        /// Get the information about the columns in a table
        /// </summary>
        /// <param name="table">The table name that you want the columns for.</param>
        /// <returns></returns>
        Column[] GetColumns(string table);

        /// <summary>
        /// Get information about a single column in a table
        /// </summary>
        /// <param name="table">The table name that you want the columns for.</param>
        /// <param name="column">The column name for which you want information.</param>
        /// <returns></returns>
        Column GetColumn(string table, string column);

        /// <summary>
        /// Check to see if a column exists
        /// </summary>
        /// <param name="table"></param>
        /// <param name="column"></param>
        /// <returns></returns>
        bool ColumnExists(string table, string column);

        /// <summary>
        /// Change the definition of an existing column.
        /// </summary>
        /// <param name="table">The name of the table that will get the new column</param>
        /// <param name="column">An instance of a <see cref="Column">Column</see> with the specified properties and the name of an existing column</param>
        void ChangeColumn(string table, Column column);

        /// <summary>
        /// Add a column to an existing table
        /// </summary>
        /// <param name="tableName">The name of the table that will get the new column</param>
        /// <param name="column">The column to be added</param>
        void AddColumn(string tableName, Column column);

        /// <summary>
        /// Remove an existing column from a table
        /// </summary>
        /// <param name="table">The name of the table to remove the column from</param>
        /// <param name="column">The column to remove</param>
        void RemoveColumn(string table, string column);

        /// <summary>
        /// Rename an existing table
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="oldColumnName">The old name of the column</param>
        /// <param name="newColumnName">The new name of the column</param>
        void RenameColumn(string tableName, string oldColumnName, string newColumnName);

        #endregion

        #region Constraints

        /// <summary>
        /// Add a foreign key constraint
        /// </summary>
        void AddForeignKey(ForeignKey foreignKey);

        /// <summary>
        /// Remove an existing foreign key constraint
        /// </summary>
        /// <param name="table">The table that contains the foreign key.</param>
        /// <param name="name">The name of the foreign key to remove</param>
        void RemoveForeignKey(string table, string name);

        /// <summary>
        /// Check to see if a constraint exists
        /// </summary>
        /// <param name="name">The name of the constraint</param>
        /// <param name="table">The table that the constraint lives on.</param>
        /// <returns></returns>
        bool ConstraintExists(string table, string name);

        /// <summary>
        /// Add a constraint to a table
        /// </summary>
        /// <param name="name">The name of the constraint to add.</param>
        /// <param name="table">The name of the table that will get the constraint</param>
        /// <param name="columns">The name of the column or columns that will get the constraint.</param>
        void AddUniqueConstraint(string name, string table, params string[] columns);

        /// <summary>
        /// Add a constraint to a table
        /// </summary>
        /// <param name="name">The name of the constraint to add.</param>
        /// <param name="table">The name of the table that will get the constraint</param>
        /// <param name="checkSql">The check constraint definition.</param>
        void AddCheckConstraint(string name, string table, string checkSql);

        /// <summary>
        /// Remove an existing constraint
        /// </summary>
        /// <param name="table">The table that contains the foreign key.</param>
        /// <param name="name">The name of the constraint to remove</param>
        void RemoveConstraint(string table, string name);


        /// <summary>
        /// Check to see if a primary key constraint exists on the table
        /// </summary>
        /// <param name="name">The name of the primary key</param>
        /// <param name="table">The table that the constraint lives on.</param>
        /// <returns></returns>
        bool PrimaryKeyExists(string table, string name);

        /// <summary>
        /// Add a primary key to a table
        /// </summary>
        /// <param name="tableName">The name of the table that will get the primary key.</param>
        /// <param name="columnName">The name of the column that will be the primary key.</param>
        void AddPrimaryKey(string tableName, string columnName);

        #endregion

        #region Data

        /// <summary>
        /// Insert data into a table
        /// </summary>
        /// <param name="table">The table that will get the new data</param>
        /// <param name="columns">The names of the columns</param>
        /// <param name="values">The values in the same order as the columns</param>
        /// <returns></returns>
        int Insert(string table, string[] columns, string[] values);

        /// <summary>
        /// Update the values in a table
        /// </summary>
        /// <param name="table">The name of the table to update</param>
        /// <param name="columns">The names of the columns.</param>
        /// <param name="values">The values for the columns in the same order as the names.</param>
        /// <param name="where">A where clause to limit the update</param>
        /// <returns></returns>
        int Update(string table, string[] columns, string[] values, string where);

        /// <summary>
        /// Delete data from a table
        /// </summary>
        /// <param name="table">The table that will have the data deleted</param>
        /// <param name="whereColumn">The name of the column used in a where clause</param>
        /// <param name="whereValue">The value for the where clause</param>
        /// <returns></returns>
        int DeleteData(string table, string whereColumn, string whereValue);

        /// <summary>
        /// Delete data from a table
        /// </summary>
        /// <param name="table">The table that will have the data deleted</param>
        /// <param name="columns">The names of the columns used in a where clause</param>
        /// <param name="values">The values in the same order as the columns</param>
        /// <returns></returns>
        int DeleteData(string table, string[] columns, string[] values);

        #endregion

        #region Migrations

        /// <summary>
        /// Marks a Migration version number as having been applied
        /// </summary>
        /// <param name="version">The version number of the migration that was applied</param>
        void MigrationApplied(long version);

        /// <summary>
        /// Marks a Migration version number as having been rolled back from the database
        /// </summary>
        /// <param name="version">The version number of the migration that was removed</param>
        void MigrationUnApplied(long version);

        #endregion

        #region Indexes

        string AddIndex(string table, params string[] columns);
        List<string> GetIndexes(string tableName);

        #endregion

        #region Execute

        /// <summary>
        /// Execute an arbitrary SQL query
        /// </summary>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="args"> </param>
        /// <returns>A single value that is returned.</returns>
        object ExecuteScalar(string sql, params object[] args);

        /// <summary>
        /// Get a single value from a table
        /// </summary>
        /// <param name="what">The columns to select</param>
        /// <param name="from">The table to select from</param>
        /// <param name="where"></param>
        /// <returns></returns>
        object SelectScalar(string what, string from, string where);

        /// <summary>
        /// Execute an arbitrary SQL query
        /// </summary>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="args"> </param>
        /// <returns></returns>
        int ExecuteNonQuery(string sql, params object[] args);

        /// <summary>
        /// Get values from a table
        /// </summary>
        /// <param name="what">The columns to select</param>
        /// <param name="from">The table to select from</param>
        /// <param name="where">The where clause to limit the selection</param>
        /// <returns></returns>
        IDataReader Select(string what, string from, string where);

        /// <summary>
        /// Execute an arbitrary SQL query
        /// </summary>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="args"> </param>
        /// <returns></returns>
        IDataReader ExecuteQuery(string sql, params object[] args);

        List<string> ExecuteStringQuery(string sql, params object[] args);
        #endregion

        #region Transaction

        /// <summary>
        /// Start a transaction
        /// </summary>
        void BeginTransaction();

        /// <summary>
        /// Commit the running transaction
        /// </summary>
        void Commit();

        /// <summary>
        /// Rollback the currently running transaction.
        /// </summary>
        void Rollback();

        #endregion
    }
}