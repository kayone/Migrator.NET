using System.Data;
using Migrator.Providers.SqlServer;

namespace Migrator.Providers.SqlServerCe
{
	public class SqlServerCeDialect : SqlServerDialect
	{
		public SqlServerCeDialect()
		{
			RegisterColumnType(DbType.AnsiStringFixedLength, "NCHAR(255)");
			RegisterColumnType(DbType.AnsiStringFixedLength, 4000, "NCHAR($l)");
			RegisterColumnType(DbType.AnsiString, "NVARCHAR(255)");
			RegisterColumnType(DbType.AnsiString, 4000, "NVARCHAR($l)");
			RegisterColumnType(DbType.AnsiString, 1073741823, "TEXT");
            RegisterColumnType(DbType.AnsiString, 2147483647, "TEXT");
            RegisterColumnType(DbType.Double, "FLOAT");
            RegisterColumnType(DbType.String, 1073741823, "NTEXT");
            RegisterColumnType(DbType.Binary, 2147483647, "IMAGE");
		}


        public override bool SupportsMultiDb
        {
            get { return false; }
        }
	}
}