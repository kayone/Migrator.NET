using System;
using System.Collections.Generic;
using System.Data;
using FluentAssertions;
using Kayone.TestFoundation;
using Migrator.Framework;
using Migrator.Framework.Exceptions;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithoutConstraints
{
    public abstract class TransformationProviderTestBase<TProvider> : LoggingTest
        where TProvider : TransformationProviderBase
    {
        protected const string TEST_DB_NAME = "MigUnitTest";

        protected const string TestTableName = "TestTable";
        protected const string TestTableWithIdName = "TestTableWithId";
        protected const string TestTableWithPkName = "TestTableWithPk";

        protected const string IdColumnName = "Id";

        protected readonly Column BigStringColumb = new Column("BigStringColumb", DbType.String, 50000,
                                                               ColumnProperty.Null);


        protected readonly Column BinColumn = new Column("BinColumn", DbType.Binary, ColumnProperty.Null);
        protected readonly Column BoolColumn = new Column("BoolColumn", DbType.Boolean, ColumnProperty.Null);
        protected readonly Column NameColumn = new Column("NameColumn", DbType.String, 50, ColumnProperty.Null);
        protected readonly Column NumberColumn = new Column("NumberColumn", DbType.Int32, ColumnProperty.Null);
        protected readonly Column TitleColumn = new Column("TitleColumn", DbType.String, 100, ColumnProperty.Null);
        protected TProvider _provider;
        protected abstract TProvider Provider();

        [SetUp]
        public void TransformationProviderBaseSetup()
        {
            _provider = Provider();

            if (_provider.Dialect.SupportsMultiDb)
            {
                if (_provider.DatabaseExists(TEST_DB_NAME))
                {
                    _provider.DropDatabases(TEST_DB_NAME);
                }

                _provider.CreateDatabases(TEST_DB_NAME);
                _provider.SwitchDatabase(TEST_DB_NAME);
            }
            else
            {
                _provider.WipeDatabase(_provider.Connection.Database);
            }

            GivenTestTable();
        }

        [TearDown]
        public void TransformationProviderBaseTearDown()
        {
            ExceptionVerification.Clear();
            _provider.Dispose();
        }


        public void GivenTestTable()
        {
            _provider.AddTable(TestTableName,
                               new Column(IdColumnName, DbType.Int32, ColumnProperty.NotNull),
                               NumberColumn,
                               TitleColumn,
                               NameColumn,
                               BinColumn,
                               BoolColumn,
                               BigStringColumb);
        }

        public void GivenTableWithIdentity()
        {
            _provider.AddTable(TestTableWithIdName,
                               new Column(IdColumnName, DbType.Int32, ColumnProperty.PrimaryKeyWithIdentity),
                               TitleColumn,
                               NumberColumn,
                               NameColumn,
                               BinColumn,
                               BoolColumn,
                               BigStringColumb);
        }

        public void GivenTableWithPrimaryKey()
        {
            _provider.AddTable(TestTableWithPkName,
                               new Column(IdColumnName, DbType.Int32, ColumnProperty.PrimaryKey),
                               TitleColumn,
                               NumberColumn,
                               NameColumn,
                               BinColumn,
                               BoolColumn,
                               BigStringColumb);
        }

        [Test]
        public void TableExists_should_not_fined_invalid_table()
        {
            _provider.TableExists("BadName").Should().BeFalse();
        }

        [Test]
        public void TableExists_should_find_test_table()
        {
            _provider.TableExists(TestTableName).Should().BeTrue();
        }

        [Test]
        public void ColumnExists_should_find_existing_column()
        {
            _provider.ColumnExists(TestTableName, TitleColumn.Name).Should().BeTrue();
        }

        [Test]
        public void ColumnExists_should_not_find_invalid_column()
        {
            _provider.ColumnExists(TestTableName, "BadName").Should().BeFalse();
        }

        [Test]
        public void TableCanBeAdded()
        {
            _provider.TableExists(TestTableName).Should().BeTrue();
        }

        [Test]
        public void GetTablesWorks()
        {
            _provider.GetTables().Should().HaveCount(1);

            GivenTableWithIdentity();
            _provider.GetTables().Should().HaveCount(2);
        }

        [Test]
        public void GetColumnsReturnsProperCount()
        {
            var cols = _provider.GetColumns(TestTableName);
            cols.Should().HaveCount(7);
        }

        [Test]
        public void GetColumnsContainsProperNullInformation()
        {
            GivenTableWithPrimaryKey();

            var cols = _provider.GetColumns(TestTableName);
            cols.Should().NotBeEmpty();

            foreach (var column in cols)
            {
                if (column.Name == IdColumnName)
                {
                    column.ColumnProperty.Should().Be(ColumnProperty.NotNull);
                }
                else if (column.Name == TitleColumn.Name)
                {
                    column.ColumnProperty.Should().Be(ColumnProperty.Null);
                }
            }
        }

        [Test]
        public void CanAddTableWithPrimaryKey()
        {
            GivenTableWithPrimaryKey();
            _provider.TableExists(TestTableWithPkName).Should().BeTrue();
        }

        [Test]
        public void CanAddTableWithIdentity()
        {
            GivenTableWithIdentity();
            _provider.TableExists(TestTableWithIdName).Should().BeTrue();
        }

        [Test]
        public void RemoveTable()
        {
            _provider.TableExists(TestTableName).Should().BeTrue();

            _provider.DropTable(TestTableName);
            _provider.TableExists(TestTableName).Should().BeFalse();
        }

        [Test]
        public void RenameTableThatExists()
        {
            _provider.RenameTable(TestTableName, "Test_Rename");

            _provider.TableExists("Test_Rename").Should().BeTrue();
            _provider.TableExists(TestTableName).Should().BeFalse();
        }

        [Test]
        public void RenameTableToExistingTable()
        {
            GivenTableWithIdentity();
            Assert.Throws<TableAlreadyExistsException>(() => _provider.RenameTable(TestTableName, TestTableWithIdName));
        }

        [Test]
        public void RenameColumnThatExists()
        {
            _provider.RenameColumn(TestTableName, NameColumn.Name, "RenamedColumn");

            _provider.ColumnExists(TestTableName, "RenamedColumn").Should().BeTrue();
            _provider.ColumnExists(TestTableName, NameColumn.Name).Should().BeFalse();
        }

        [Test]
        [ExpectedException(typeof (ColumnAlreadyExistsException))]
        public void RenameColumnToExistingColumn()
        {
            _provider.RenameColumn(TestTableName, TitleColumn.Name, NameColumn.Name);
        }

        [Test]
        public void RemoveUnexistingTable()
        {
            _provider.DropTable(TestTableName);
            _provider.TableExists(TestTableName).Should().BeFalse();
        }

        [Test]
        public void AddColumn()
        {
            _provider.AddColumn(TestTableName, new Column("TestCol", DbType.String, 50));
            _provider.ColumnExists(TestTableName, "TestCol").Should().BeTrue();
        }


        [Test]
        public void ChangeExistingColumn()
        {
            _provider.ChangeColumn(TestTableName, new Column(TitleColumn.Name, DbType.String, 50));
            _provider.ColumnExists(TestTableName, TitleColumn.Name);
        }

        [Test]
        public void AddDecimalColumn()
        {
            _provider.AddColumn(TestTableName, new Column("TestDecimal", DbType.Decimal, 38));
            _provider.ColumnExists(TestTableName, "TestDecimal").Should().BeTrue();
        }

        [Test]
        public void AddColumnWithDefault()
        {
            _provider.AddColumn(TestTableName, new Column("TestWithDefault", DbType.Int32, 50, 0, 10));
            _provider.ColumnExists(TestTableName, "TestWithDefault").Should().BeTrue();
        }

        [Test]
        public void AddColumnWithDefaultButNoSize()
        {
            _provider.AddColumn(TestTableName, new Column("TestWithDefault", DbType.Int32, 10));
            _provider.ColumnExists(TestTableName, "TestWithDefault").Should().BeTrue();

            _provider.AddColumn(TestTableName, new Column("TestWithDefaultString", DbType.String, "'foo'"));
            _provider.ColumnExists(TestTableName, "TestWithDefaultString").Should().BeTrue();
        }

        [Test]
        public void AddBooleanColumnWithDefault()
        {
            _provider.AddColumn(TestTableName, new Column("TestBoolean", DbType.Boolean, 0, 0, false));
            _provider.ColumnExists(TestTableName, "TestBoolean").Should().BeTrue();
        }

        [Test]
        public void CanGetNullableFromProvider()
        {
            _provider.AddColumn(TestTableName, new Column("NullableColumn", DbType.String, 30, ColumnProperty.Null));
            var columns = _provider.GetColumns(TestTableName);
            foreach (var column in columns)
            {
                if (column.Name == "NullableColumn")
                {
                    Assert.IsTrue((column.ColumnProperty & ColumnProperty.Null) == ColumnProperty.Null);
                }
            }
        }

        [Test]
        public void RemoveColumn()
        {
            _provider.RemoveColumn(TestTableName, TitleColumn.Name);
            _provider.ColumnExists(TestTableName, TitleColumn.Name).Should().BeFalse();
        }

        [Test]
        public virtual void RemoveColumnWithDefault()
        {
            AddColumnWithDefault();
            _provider.RemoveColumn(TestTableName, "TestWithDefault");
            Assert.IsFalse(_provider.ColumnExists(TestTableName, "TestWithDefault"));
        }

        [Test]
        public void RemoveUnexistingColumn()
        {
            Assert.Throws<ColumnDoesntExistsException>(() => _provider.RemoveColumn(TestTableName, "abc"));
            Assert.Throws<TableDoesntExistsException>(() => _provider.RemoveColumn("abc", "abc"));
        }

        [Test]
        public void RemoveBoolColumn()
        {
            _provider.AddColumn(TestTableName, new Column("Inactif", DbType.Boolean));
            _provider.ColumnExists(TestTableName, "Inactif").Should().BeTrue();

            _provider.RemoveColumn(TestTableName, "Inactif");
            _provider.ColumnExists(TestTableName, "Inactif").Should().BeFalse();
        }

        [Test]
        public void HasColumn()
        {
            _provider.ColumnExists(TestTableName, NameColumn.Name).Should().BeTrue();
            _provider.ColumnExists(TestTableName, TitleColumn.Name).Should().BeTrue();
        }

        [Test]
        public void HasTable()
        {
            Assert.IsTrue(_provider.TableExists(TestTableName));
        }

        [Test]
        public void AppliedMigrations()
        {
            Assert.IsFalse(_provider.TableExists("SchemaInfo"));

            // Check that a "get" call works on the first run.
            Assert.AreEqual(0, _provider.AppliedMigrations.Count);
            Assert.IsTrue(_provider.TableExists("SchemaInfo"), "No SchemaInfo table created");

            // Check that a "set" called after the first run works.
            _provider.MigrationApplied(1);
            Assert.AreEqual(1, _provider.AppliedMigrations[0]);

            _provider.DropTable("SchemaInfo");
            // Check that a "set" call works on the first run.
            _provider.MigrationApplied(1);
            Assert.AreEqual(1, _provider.AppliedMigrations[0]);
            Assert.IsTrue(_provider.TableExists("SchemaInfo"), "No SchemaInfo table created");
        }

        /// <summary>
        ///   Reproduce bug reported by Luke Melia & Daniel Berlinger : http://macournoyer.wordpress.com/2006/10/15/migrate-nant-task/#comment-113
        /// </summary>
        [Test]
        public void CommitTwice()
        {
            _provider.Commit();
            Assert.AreEqual(0, _provider.AppliedMigrations.Count);
            _provider.Commit();
        }

        [Test]
        public void InsertData()
        {
            _provider.Insert(TestTableName, new[] {IdColumnName, NameColumn.Name}, new[] {"1", "Name1"});
            _provider.Insert(TestTableName, new[] {IdColumnName, NameColumn.Name}, new[] {"2", "Name2"});


            var data = ReadStringData(TestTableName, NameColumn.Name);

            data.Should().HaveCount(2);
            data.Should().Contain("Name1");
            data.Should().Contain("Name2");
        }

        [Test]
        public void CanInsertNullData()
        {
            _provider.Insert(TestTableName, new[] {IdColumnName, NameColumn.Name}, new[] {"1", "foo"});
            _provider.Insert(TestTableName, new[] {IdColumnName, NameColumn.Name}, new[] {"2", null});


            var data = ReadStringData(TestTableName, NameColumn.Name);

            data.Should().HaveCount(2);
            data.Should().Contain("foo");
            data.Should().Contain(c => c == null);
        }

        [Test]
        public void CanInsertDataWithSingleQuotes()
        {
            _provider.Insert(TestTableName, new[] {IdColumnName, TitleColumn.Name}, new[] {"1", "Muad'Dib"});

            var data = ReadStringData(TestTableName, TitleColumn.Name);

            data.Should().HaveCount(1);
            data.Should().Contain("Muad'Dib");
        }

        [Test]
        public void DeleteData()
        {
            InsertData();
            _provider.DeleteData(TestTableName, IdColumnName, "1");

            var data = ReadStringData(TestTableName, NameColumn.Name);

            data.Should().HaveCount(1);
            data.Should().Contain("Name2");
        }

        [Test]
        public void DeleteDataWithArrays()
        {
            InsertData();
            _provider.DeleteData(TestTableName, new[] {IdColumnName}, new[] {"1"});

            var data = ReadStringData(TestTableName, NameColumn.Name);

            data.Should().HaveCount(1);
            data.Should().Contain("Name2");
        }

        [Test]
        public void UpdateData()
        {
            InsertData();

            _provider.Update(TestTableName, new[] {IdColumnName}, new[] {"3"});

            var data = ReadIntData(TestTableName, IdColumnName);

            data.Should().HaveCount(2);
            data.Should().OnlyContain(c => c == 3);
        }

        [Test]
        public void CanUpdateWithNullData()
        {
            InsertData();

            _provider.Update(TestTableName, new[] {IdColumnName, NameColumn.Name}, new[] {"3", null});

            var ids = ReadIntData(TestTableName, IdColumnName);
            var names = ReadStringData(TestTableName, NameColumn.Name);

            ids.Should().HaveCount(2);
            ids.Should().OnlyContain(c => c == 3);

            names.Should().HaveCount(2);
            names.Should().OnlyContain(c => c == null);
        }

        [Test]
        public void UpdateDataWithWhere()
        {
            InsertData();

            _provider.Update(TestTableName, new[] {IdColumnName}, new[] {"3"}, IdColumnName + "='1'");

            var data = ReadIntData(TestTableName, IdColumnName);

            data.Should().HaveCount(2);
            data.Should().NotContain(c => c == 1);
            data.Should().Contain(c => c == 3);
        }


        private List<string> ReadStringData(string tableName, string column)
        {
            var values = new List<string>();

            using (IDataReader reader = _provider.Select(column, tableName))
            {
                while (reader.Read())
                {
                    object value = reader[0];

                    if (value == null || value == DBNull.Value)
                    {
                        values.Add(null);
                    }
                    else
                    {
                        values.Add(value.ToString());
                    }
                }
            }

            return values;
        }

        private List<int> ReadIntData(string tableName, string column)
        {
            var values = new List<int>();

            using (IDataReader reader = _provider.Select(column, tableName))
            {
                while (reader.Read())
                {
                    object value = reader[0];

                    if (value == null || value == DBNull.Value)
                    {
                        values.Add(0);
                    }
                    else
                    {
                        values.Add(Convert.ToInt32(value));
                    }
                }
            }

            return values;
        }
    }
}