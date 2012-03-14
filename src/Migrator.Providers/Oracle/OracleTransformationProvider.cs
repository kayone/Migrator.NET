using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Migrator.Framework;
using Oracle.DataAccess.Client;
using ForeignKeyConstraint = Migrator.Framework.ForeignKeyConstraint;

namespace Migrator.Providers.Oracle
{
	public class OracleTransformationProvider : TransformationProvider
	{
		public const string TemporaryColumnName = "TEMPCOL";

		public OracleTransformationProvider(Dialect dialect, string connectionString)
			: base(dialect, connectionString)
		{
			_connection = new OracleConnection();
			_connection.ConnectionString = _connectionString;
			_connection.Open();
		}

		public override void AddForeignKey(string name, string primaryTable, string[] primaryColumns, string refTable,
		                                   string[] refColumns, ForeignKeyConstraint constraint)
		{
			GuardAgainstMaximumIdentifierLengthForOracle(name);

			if (ConstraintExists(primaryTable, name))
			{
				Logger.Warn("Constraint {0} already exists", name);
				return;
			}

			primaryTable = QuoteTableNameIfRequired(primaryTable);
			refTable = QuoteTableNameIfRequired(refTable);
			string primaryColumnsSql = String.Join(",", primaryColumns.Select(col => QuoteColumnNameIfRequired(col)).ToArray());
			string refColumnsSql = String.Join(",", refColumns.Select(col => QuoteColumnNameIfRequired(col)).ToArray());

			ExecuteNonQuery(String.Format("ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY ({2}) REFERENCES {3} ({4})", primaryTable, name, primaryColumnsSql, refTable, refColumnsSql));
		}

		void GuardAgainstMaximumIdentifierLengthForOracle(string name)
		{
			if (name.Length > 30)
			{
				throw new ArgumentException(string.Format("The name \"{0}\" is {1} characters in length, bug maximum length for Oracle identifier is 30 characters.", name, name.Length), "name");
			}
		}

		public override void ChangeColumn(string table, Column column)
		{
			if (!ColumnExists(table, column.Name))
			{
				Logger.Warn("Column {0}.{1} does not exist", table, column.Name);
				return;
			}

			if (column.Type == DbType.String)
			{
				RenameColumn(table, column.Name, TemporaryColumnName);

				// check if this is not-null
				bool isNotNull = (column.ColumnProperty & ColumnProperty.NotNull) == ColumnProperty.NotNull;

				// remove the not-null option
				column.ColumnProperty = (column.ColumnProperty & ~ColumnProperty.NotNull);

				AddColumn(table, column);
				CopyDataFromOneColumnToAnother(table, TemporaryColumnName, column.Name);
				RemoveColumn(table, column.Name);
				RenameColumn(table, TemporaryColumnName, column.Name);
				string columnName = QuoteColumnNameIfRequired(column.Name);

				using (IDataReader reader = ExecuteQuery("SELECT * FROM " + QuoteTableNameIfRequired(table)))
				{
					while (reader.Read())
					{
						Console.WriteLine(reader[0]);
					}
				}

				// now set the column to not-null
				if (isNotNull) ExecuteQuery(String.Format("ALTER TABLE {0} MODIFY ({1} NOT NULL)", table, columnName));
			}
			else
			{
				ColumnPropertiesMapper mapper = _dialect.GetAndMapColumnProperties(column);
				ChangeColumn(table, mapper.ColumnSql);
			}
		}

		void CopyDataFromOneColumnToAnother(string table, string fromColumn, string toColumn)
		{
			table = QuoteTableNameIfRequired(table);
			fromColumn = QuoteColumnNameIfRequired(fromColumn);
			toColumn = QuoteColumnNameIfRequired(toColumn);
			ExecuteNonQuery(string.Format("UPDATE {0} SET {1} = {2}", table, fromColumn, toColumn));
		}

		public override void RenameTable(string oldName, string newName)
		{
			GuardAgainstMaximumIdentifierLengthForOracle(newName);

			oldName = QuoteTableNameIfRequired(oldName);
			newName = QuoteTableNameIfRequired(newName);
			ExecuteNonQuery(String.Format("ALTER TABLE {0} RENAME TO {1}", oldName, newName));
		}

		public override void RenameColumn(string tableName, string oldColumnName, string newColumnName)
		{
			GuardAgainstMaximumIdentifierLengthForOracle(newColumnName);

			tableName = QuoteTableNameIfRequired(tableName);
			oldColumnName = QuoteColumnNameIfRequired(oldColumnName);
			newColumnName = QuoteColumnNameIfRequired(newColumnName);
			ExecuteNonQuery(string.Format("ALTER TABLE {0} RENAME COLUMN {1} TO {2}", tableName, oldColumnName, newColumnName));
		}

		public override void ChangeColumn(string table, string sqlColumn)
		{
			if (string.IsNullOrEmpty(table)) throw new ArgumentNullException("table");
			if (string.IsNullOrEmpty(table)) throw new ArgumentNullException("sqlColumn");

			table = QuoteTableNameIfRequired(table);
			sqlColumn = QuoteColumnNameIfRequired(sqlColumn);
			ExecuteNonQuery(String.Format("ALTER TABLE {0} MODIFY {1}", table, sqlColumn));
		}

		public override void AddColumn(string table, string sqlColumn)
		{
			GuardAgainstMaximumIdentifierLengthForOracle(table);
			table = QuoteTableNameIfRequired(table);
			sqlColumn = QuoteColumnNameIfRequired(sqlColumn);
			ExecuteNonQuery(String.Format("ALTER TABLE {0} ADD {1}", table, sqlColumn));
		}

		public override bool ConstraintExists(string table, string name)
		{
			string sql =
				string.Format(
					"SELECT COUNT(constraint_name) FROM user_constraints WHERE lower(constraint_name) = '{0}' AND lower(table_name) = '{1}'",
					name.ToLower(), table.ToLower());
			Logger.Log(sql);
			object scalar = ExecuteScalar(sql);
			return Convert.ToInt32(scalar) == 1;
		}

		public override bool ColumnExists(string table, string column)
		{
			if (!TableExists(table))
				return false;

			string sql =
				string.Format(
					"SELECT COUNT(column_name) FROM user_tab_columns WHERE lower(table_name) = '{0}' AND lower(column_name) = '{1}'",
					table.ToLower(), column.ToLower());
			Logger.Log(sql);
			object scalar = ExecuteScalar(sql);
			return Convert.ToInt32(scalar) == 1;
		}

		public override bool TableExists(string table)
		{
			string sql = string.Format("SELECT COUNT(table_name) FROM user_tables WHERE lower(table_name) = '{0}'",
			                           table.ToLower());
			Logger.Log(sql);
			object count = ExecuteScalar(sql);
			return Convert.ToInt32(count) == 1;
		}

		public override string[] GetTables()
		{
			var tables = new List<string>();

			using (IDataReader reader =
				ExecuteQuery("SELECT table_name FROM user_tables"))
			{
				while (reader.Read())
				{
					tables.Add(reader[0].ToString());
				}
			}

			return tables.ToArray();
		}

		public override Column[] GetColumns(string table)
		{
			var columns = new List<Column>();

			using (
				IDataReader reader =
					ExecuteQuery(
						string.Format(
							"select column_name, data_type, data_length, data_precision, data_scale FROM USER_TAB_COLUMNS WHERE lower(table_name) = '{0}'",
							table)))
			{
				while (reader.Read())
				{
					string colName = reader[0].ToString();
					DbType colType = DbType.String;
					string dataType = reader[1].ToString().ToLower();
					if (dataType.Equals("number"))
					{
						int precision = reader.GetInt32(3);
						int scale = reader.GetInt32(4);
						if (scale == 0)
						{
							colType = precision <= 10 ? DbType.Int16 : DbType.Int64;
						}
						else
						{
							colType = DbType.Decimal;
						}
					}
					else if (dataType.StartsWith("timestamp") || dataType.Equals("date"))
					{
						colType = DbType.DateTime;
					}
					columns.Add(new Column(colName, colType));
				}
			}

			return columns.ToArray();
		}

		protected override string GenerateParameterName(int index)
		{
			return ":p" + index;
		}

		protected override void ConfigureParameterWithValue(IDbDataParameter parameter, int index, object value)
		{
			if (value is Guid || value is Guid?)
			{
				parameter.DbType = DbType.Binary;

				if (value is Guid? && !((Guid?) value).HasValue)
				{
					return;
				}

				parameter.Value = ((Guid) value).ToByteArray();
			}
			else if (value is bool || value is bool?)
			{
				parameter.DbType = DbType.Int32;
				parameter.Value = ((bool) value) ? 1 : 0;
			}
			else
			{
				base.ConfigureParameterWithValue(parameter, index, value);
			}
		}

		public override void AddTable(string name, params Column[] columns)
		{
			GuardAgainstMaximumIdentifierLengthForOracle(name);

			GuardAgainstMaximumColumnNameLengthForOracle(name, columns);

			base.AddTable(name, columns);
		}

		void GuardAgainstMaximumColumnNameLengthForOracle(string name, Column[] columns)
		{
			foreach (Column column in columns)
			{
				if (column.Name.Length > 30)
				{
					throw new ArgumentException(
						string.Format("When adding table: \"{0}\", the column: \"{1}\", the name of the column is: {2} characters in length, but maximum length for an oracle identifier is 30 characters", name,
						              column.Name, column.Name.Length), "columns");
				}
			}
		}

		public override string Encode(Guid guid)
		{
			byte[] bytes = guid.ToByteArray();
			var hex = new StringBuilder(bytes.Length*2);
			foreach (byte b in bytes) hex.AppendFormat("{0:X2}", b);
			return hex.ToString();
		}
	}
}