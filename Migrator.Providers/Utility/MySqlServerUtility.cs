using System;
using System.Data;
using MySql.Data.MySqlClient;

namespace Migrator.Providers.Utility
{
    public static class MySqlServerUtility
    {
        public static void RemoveAllTablesFromDefaultDatabase(string connectionString)
        {
            using (MySqlConnection connection = new MySqlConnection(connectionString))
            {
                connection.Open();

                string dropAllTablesSql = null;

                do
                {
                    dropAllTablesSql = GetDropAllTablesSql(connection);

                    if (dropAllTablesSql == null) continue;

                    DisableForeignKeys(connection);

                    ExecuteDropCommand(connection, dropAllTablesSql);
                } while (dropAllTablesSql != null);
            }
        }

        private static void ExecuteDropCommand(MySqlConnection connection, string dropAllTablesSql)
        {
            using (MySqlCommand dropCmd = new MySqlCommand(dropAllTablesSql, connection))
            {
                dropCmd.ExecuteNonQuery();
            }
        }

        public static void DisableForeignKeys(MySqlConnection connection)
        {
            using (MySqlCommand command = new MySqlCommand("Set foreign_key_checks=off;", connection))
            {
                command.ExecuteNonQuery();
            }
        }

        public static string GetDropAllTablesSql(MySqlConnection connection)
        {
            const string query =
                @"set group_concat_max_len=10240;
SELECT concat('DROP TABLE IF EXISTS ', group_concat(table_name)) drop_statement
FROM information_schema.tables
WHERE table_schema=database();";

            using (MySqlCommand getDropAllTablesCommand = new MySqlCommand(query, connection))
            {
                getDropAllTablesCommand.CommandType = CommandType.Text;

                using (MySqlDataReader reader = getDropAllTablesCommand.ExecuteReader())
                {
                    if (reader.Read() && (reader[0] != null && !Convert.IsDBNull(reader[0])))
                    {
                        return reader[0].ToString();
                    }
                }
            }

            return null;
        }
    }
}