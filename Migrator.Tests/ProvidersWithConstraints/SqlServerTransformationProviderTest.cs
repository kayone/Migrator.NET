using System.Data;
using FluentAssertions;
using Migrator.Framework;
using Migrator.Providers.SqlServer;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithConstraints
{
    [TestFixture]
    [Category("SqlServer")]
    public class SqlServerTransformationProviderTest :
        TransformationProviderWithConstraintTestBase<SqlServerTransformationProvider>
    {
        protected override SqlServerTransformationProvider Provider()
        {
            return new SqlServerTransformationProvider(ConnectionString);
        }

        public virtual string ConnectionString
        {
            get { return @"Data Source=localhost\SQLExpress;Integrated Security=True;Database=Master;"; }
        }

        [Test]
        public void ByteColumnWillBeCreatedAsBlob()
        {
            _provider.AddColumn(TestTableName, new Column("BlobColumn", DbType.Byte));
            _provider.ColumnExists(TestTableName, "BlobColumn").Should().BeTrue();
        }

        [Test]
        public void QuoteCreatesProperFormat()
        {
            Dialect dialect = new SqlServerDialect();
            dialect.Quote("foo").Should().Be("[foo]");
        }

        [Test, Ignore("Not implemented yet")]
        public override void RemoveColumnWithDefault()
        {
            base.AddRemoveForeignKey();
        }

        [Test]
        public void TableExistsShouldWorkWithBracketsAndSchemaNameAndTableName()
        {
            _provider.TableExists("[dbo].[TestTable]").Should().BeTrue();
        }

        [Test]
        public void TableExistsShouldWorkWithSchemaNameAndTableName()
        {
            Assert.IsTrue(_provider.TableExists("dbo.TestTable"));
        }

        [Test]
        public void TableExistsShouldWorkWithTableNamesWithBracket()
        {
            Assert.IsTrue(_provider.TableExists("[TestTable]"));
        }
    }
}