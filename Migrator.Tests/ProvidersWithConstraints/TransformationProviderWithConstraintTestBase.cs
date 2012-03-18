using System.Data;
using FluentAssertions;
using Migrator.Framework;
using Migrator.Providers;
using Migrator.Tests.ProvidersWithoutConstraints;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithConstraints
{
    /// <summary>
    /// Base class for Provider tests for all tests including constraint oriented tests.
    /// </summary>
    public abstract class TransformationProviderWithConstraintTestBase<TProvider> : TransformationProviderTestBase<TProvider> where TProvider : TransformationProviderBase
    {

        private const string CustomersTableName = "Customers";
        private const string OrderTableName = "Orders";

        protected const string CustomerIdCol = "CustomerId";
        protected const string OrderIdCol = "OrderId";
        protected const string OrderForeignKey = "OrderCustomerId";

        public void GivenCustomersTables()
        {
            _provider.AddTable(CustomersTableName,
                               new Column(CustomerIdCol, DbType.Int32, ColumnProperty.PrimaryKeyWithIdentity),
                               NameColumn);

            _provider.AddTable(OrderTableName,
                    new Column(OrderIdCol, DbType.Int32, ColumnProperty.PrimaryKeyWithIdentity),
                    new Column(OrderForeignKey, DbType.Int32, ColumnProperty.NotNull));
        }


        private string AddForeignKey()
        {
            GivenCustomersTables();

            return _provider.AddForeignKey(new ForeignKey(OrderTableName, OrderForeignKey, CustomersTableName, CustomerIdCol)).Name;
        }

        private string AddPrimaryKey()
        {
            return _provider.AddPrimaryKey(TestTableName, IdColumnName);
        }

        private string AddUniqueConstraint()
        {
            return _provider.AddUniqueConstraint(TestTableName, NameColumn.Name);
        }

        private string AddMultipleUniqueConstraint()
        {
            return _provider.AddUniqueConstraint(TestTableName, NameColumn.Name, TitleColumn.Name);
        }

        private string AddCheckConstraint()
        {
            return _provider.AddCheckConstraint("CK_TestTwo_TestId", TestTableName, NumberColumn.Name + ">5");
        }

        [Test]
        public void CanAddPrimaryKey()
        {
            var name = AddPrimaryKey();
            _provider.PrimaryKeyExists(TestTableName, name).Should().BeTrue();
        }


        [Test]
        public void AddUniqueColumn()
        {
            _provider.AddColumn(TestTableName, new Column("Test", DbType.String, 50, ColumnProperty.Unique));
        }



        [Test]
        public virtual void AddRemoveUniqueConstraint()
        {
            var name = AddUniqueConstraint();
            _provider.ConstraintExists(TestTableName, name).Should().BeTrue();

            _provider.RemoveConstraint(TestTableName, name);
            _provider.ConstraintExists(TestTableName, name).Should().BeFalse();
        }

        [Test]
        public virtual void AddRemoveMultipleUniqueConstraint()
        {
            var name = AddMultipleUniqueConstraint();
            _provider.ConstraintExists(TestTableName, name).Should().BeTrue();

            _provider.RemoveConstraint(TestTableName, name);
            _provider.ConstraintExists(TestTableName, name).Should().BeFalse();
        }

        [Test]
        public virtual void AddRemoveCheckConstraint()
        {
            var name = AddCheckConstraint();
            _provider.ConstraintExists(TestTableName, name).Should().BeTrue();

            _provider.RemoveConstraint(TestTableName, name);
            _provider.ConstraintExists(TestTableName, name).Should().BeFalse();
        }

        [Test]
        public virtual void AddRemoveForeignKey()
        {
            var name = AddForeignKey();
            _provider.ConstraintExists(OrderTableName, name).Should().BeTrue();

            _provider.RemoveForeignKey(OrderTableName, name);
            _provider.ConstraintExists(OrderTableName, name).Should().BeFalse();
        }

   


        [Test]
        public virtual void RemoveUnexistingForeignKey()
        {
            AddForeignKey();
            _provider.RemoveForeignKey("abc", "FK_Test_TestTwo");
            _provider.RemoveForeignKey("abc", "abc");
            _provider.RemoveForeignKey("Test", "abc");
        }
    }
}