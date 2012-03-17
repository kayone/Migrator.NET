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
using System.Configuration;
using Migrator.Framework;
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
            const string testSql = "CREATE TABLE bar ( id INTEGER PRIMARY KEY AUTOINCREMENT, bar TEXT, baz INTEGER NOT NULL )";
            string[] columns = _provider.ParseSqlColumnDefs(testSql);
            Assert.IsNotNull(columns);
            Assert.AreEqual(3, columns.Length);
            Assert.AreEqual("id INTEGER PRIMARY KEY AUTOINCREMENT", columns[0]);
            Assert.AreEqual("bar TEXT", columns[1]);
            Assert.AreEqual("baz INTEGER NOT NULL", columns[2]);
        }

        [Test]
        public void CanParseSqlDefinitionsForColumnNames()
        {
            const string testSql = "CREATE TABLE bar ( id INTEGER PRIMARY KEY AUTOINCREMENT, bar TEXT, baz INTEGER NOT NULL )";
            string[] columns = _provider.ParseSqlForColumnNames(testSql);
            Assert.IsNotNull(columns);
            Assert.AreEqual(3, columns.Length);
            Assert.AreEqual("id", columns[0]);
            Assert.AreEqual("bar", columns[1]);
            Assert.AreEqual("baz", columns[2]);
        }
    }
}