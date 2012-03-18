using Migrator.Providers.SQLite;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithoutConstraints
{
    [TestFixture]
    [Category("SQLite")]
    public class SQLiteTransformationProviderTest : TransformationProviderTestBase<SQLiteTransformationProvider>
    {
        protected override SQLiteTransformationProvider Provider()
        {
            return new SQLiteTransformationProvider(ConnectionString);
        }

        public virtual string ConnectionString
        {
            get { return string.Format("Data Source=unitTest.db;Version=3;"); }
        }


        [Test]
        public void CanParseColumnDefForName()
        {
            const string nullString = "bar TEXT";
            const string notNullString = "baz INTEGER NOT NULL";
            Assert.AreEqual("bar", _provider.ExtractNameFromColumnDef(nullString));
            Assert.AreEqual("baz", _provider.ExtractNameFromColumnDef(notNullString));
        }

        [Test]
        public void CanParseColumnDefForNotNull()
        {
            const string nullString = "bar TEXT";
            const string notNullString = "baz INTEGER NOT NULL";
            Assert.IsTrue(_provider.IsNullable(nullString));
            Assert.IsFalse(_provider.IsNullable(notNullString));
        }

        [Test]
        public void CanParseSqlDefinitions()
        {
            const string testSql =
                "CREATE TABLE bar ( id INTEGER PRIMARY KEY AUTOINCREMENT, bar TEXT, baz INTEGER NOT NULL )";
            var columns = _provider.ParseSqlColumnDefs(testSql);
            Assert.IsNotNull(columns);
            Assert.AreEqual(3, columns.Length);
            Assert.AreEqual("id INTEGER PRIMARY KEY AUTOINCREMENT", columns[0]);
            Assert.AreEqual("bar TEXT", columns[1]);
            Assert.AreEqual("baz INTEGER NOT NULL", columns[2]);
        }

        [Test]
        public void CanParseSqlDefinitionsForColumnNames()
        {
            const string testSql =
                "CREATE TABLE bar ( id INTEGER PRIMARY KEY AUTOINCREMENT, bar TEXT, baz INTEGER NOT NULL )";
            var columns = _provider.ParseSqlForColumnNames(testSql);
            Assert.IsNotNull(columns);
            Assert.AreEqual(3, columns.Length);
            Assert.AreEqual("id", columns[0]);
            Assert.AreEqual("bar", columns[1]);
            Assert.AreEqual("baz", columns[2]);
        }
    }
}