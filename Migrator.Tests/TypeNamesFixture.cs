using System.Data;
using FluentAssertions;
using Migrator.Framework;
using NUnit.Framework;

namespace Migrator.Tests
{
    [TestFixture]
    public class TypeNamesFixture
    {
        [Test]
        public void TypeNames_new_default_should_replace_old_default()
        {
            TypeNames typeNames = new TypeNames();

            typeNames.Put(DbType.String, "First");
            typeNames.Put(DbType.String, "Second");

            typeNames.Get(DbType.String).Should().Be("Second");
        }

        [Test]
        public void TypeNames_should_replace_existing_if_size_and_dbType_is_same()
        {
            TypeNames typeNames = new TypeNames();

            typeNames.Put(DbType.String, 12, "First");
            typeNames.Put(DbType.String, 12, "Second");

            typeNames.Get(DbType.String, 1, 1, 1).Should().Be("Second");
        }
    }
}