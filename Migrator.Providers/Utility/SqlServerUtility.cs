using System.Data;
using System.Data.SqlClient;

namespace Migrator.Providers.Utility
{
	public static class SqlServerUtility
	{
		public static void RemoveAllTablesFromDefaultDatabase(string connectionString)
		{
			using (var connection = new SqlConnection(connectionString))
			{
				connection.Open();
				RemoveAllForeignKeys(connection);
				DropAllTables(connection);
				connection.Close();
			}
		}

		static void DropAllTables(SqlConnection connection)
		{
			ExecuteForEachTable(connection, "DROP TABLE ?");
		}

		static void RemoveAllForeignKeys(SqlConnection connection)
		{
			using (
				var dropConstraintsCommand =
					new SqlCommand(
						@"DECLARE @Sql NVARCHAR(500) DECLARE @Cursor CURSOR

SET @Cursor = CURSOR FAST_FORWARD FOR

SELECT DISTINCT sql = 'ALTER TABLE [' + tc2.TABLE_NAME + '] DROP [' + rc1.CONSTRAINT_NAME + ']'

FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc1

LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc2 ON tc2.CONSTRAINT_NAME =rc1.CONSTRAINT_NAME

OPEN @Cursor FETCH NEXT FROM @Cursor INTO @Sql

WHILE (@@FETCH_STATUS = 0)

BEGIN

Exec sys.sp_executesql @Sql

FETCH NEXT FROM @Cursor INTO @Sql

END

CLOSE @Cursor DEALLOCATE @Cursor",
						connection))
			{
				dropConstraintsCommand.CommandType = CommandType.Text;
				dropConstraintsCommand.ExecuteNonQuery();
			}
		}

		static void ExecuteForEachTable(SqlConnection connection, string command)
		{
			using (var forEachCommand = new SqlCommand("sp_MSforeachtable", connection))
			{
				forEachCommand.CommandType = CommandType.StoredProcedure;
				forEachCommand.Parameters.AddWithValue("@command1", command);
				forEachCommand.ExecuteNonQuery();
			}
		}
	}
}