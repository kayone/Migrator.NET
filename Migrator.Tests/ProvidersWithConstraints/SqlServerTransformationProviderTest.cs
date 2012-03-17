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

using System.Data;
using FluentAssertions;
using Migrator.Framework;
using Migrator.Providers;
using Migrator.Providers.SqlServer;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithConstraints
{
    [TestFixture]
    [Category("SqlServer")]
    public class SqlServerTransformationProviderTest : TransformationProviderWithConstraintTestBase<SqlServerTransformationProvider>
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
            _provider.AddColumn("TestTwo", "BlobColumn", DbType.Byte);
            Assert.IsTrue(_provider.ColumnExists("TestTwo", "BlobColumn"));
        }

        [Test]
        public void QuoteCreatesProperFormat()
        {
            Dialect dialect = new SqlServerDialect();
            dialect.Quote("foo").Should().Be("[foo]");
        }

        [Test]
        public void TableExistsShouldWorkWithBracketsAndSchemaNameAndTableName()
        {
            _provider.TableExists("[dbo].[TestTwo]").Should().BeTrue();
        }

        [Test]
        public void TableExistsShouldWorkWithSchemaNameAndTableName()
        {
            Assert.IsTrue(_provider.TableExists("dbo.TestTwo"));
        }

        [Test]
        public void TableExistsShouldWorkWithTableNamesWithBracket()
        {
            Assert.IsTrue(_provider.TableExists("[TestTwo]"));
        }
    }
}