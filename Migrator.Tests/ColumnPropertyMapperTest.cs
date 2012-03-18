using System.Data;
using Migrator.Framework;
using Migrator.Providers.SQLite;
using Migrator.Providers.SqlServer;
using NUnit.Framework;

namespace Migrator.Tests
{
    [TestFixture]
    public class ColumnPropertyMapperTest
    {
        [Test]
        public void SQLiteIndexSqlWithEmptyStringDefault()
        {
            ColumnPropertiesMapper mapper = new ColumnPropertiesMapper(new SQLiteDialect(), "varchar(30)");
            mapper.MapColumnProperties(new Column("foo", DbType.String, 1, ColumnProperty.NotNull, string.Empty));
            Assert.AreEqual("foo varchar(30) NOT NULL DEFAULT ''", mapper.ColumnSql);
        }

        [Test]
        public void SqlServerCreatesNotNullSql()
        {
            ColumnPropertiesMapper mapper = new ColumnPropertiesMapper(new SqlServerDialect(), "varchar(30)");
            mapper.MapColumnProperties(new Column("foo", DbType.String, ColumnProperty.NotNull));
            Assert.AreEqual("[foo] varchar(30) NOT NULL", mapper.ColumnSql);
        }

        [Test]
        public void SqlServerCreatesSqWithBooleanDefault()
        {
            ColumnPropertiesMapper mapper = new ColumnPropertiesMapper(new SqlServerDialect(), "bit");
            mapper.MapColumnProperties(new Column("foo", DbType.Boolean, 0, false));
            Assert.AreEqual("[foo] bit DEFAULT 0", mapper.ColumnSql);

            mapper.MapColumnProperties(new Column("bar", DbType.Boolean, 0, true));
            Assert.AreEqual("[bar] bit DEFAULT 1", mapper.ColumnSql);
        }

        [Test]
        public void SqlServerCreatesSqWithDefault()
        {
            ColumnPropertiesMapper mapper = new ColumnPropertiesMapper(new SqlServerDialect(), "varchar(30)");
            mapper.MapColumnProperties(new Column("foo", DbType.String, 0, "'NEW'"));
            Assert.AreEqual("[foo] varchar(30) DEFAULT 'NEW'", mapper.ColumnSql);
        }

        [Test]
        public void SqlServerCreatesSqWithNullDefault()
        {
            ColumnPropertiesMapper mapper = new ColumnPropertiesMapper(new SqlServerDialect(), "varchar(30)");
            mapper.MapColumnProperties(new Column("foo", DbType.String, 0, "NULL"));
            Assert.AreEqual("[foo] varchar(30) DEFAULT NULL", mapper.ColumnSql);
        }

        [Test]
        public void SqlServerCreatesSql()
        {
            ColumnPropertiesMapper mapper = new ColumnPropertiesMapper(new SqlServerDialect(), "varchar(30)");
            mapper.MapColumnProperties(new Column("foo", DbType.String, 0));
            Assert.AreEqual("[foo] varchar(30)", mapper.ColumnSql);
        }
    }
}