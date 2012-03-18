using Migrator.Framework;
using NUnit.Framework;

namespace Migrator.Tests
{
    /// <summary>
    ///   Extend this classe to test your migrations
    /// </summary>
    public abstract class MigrationsTestCase
    {
        private Migrator _migrator;

        protected abstract TransformationProviderBase TransformationProvider { get; }

        [SetUp]
        public void SetUp()
        {
            _migrator = new Migrator(TransformationProvider);

            Assert.IsTrue(_migrator.MigrationsTypes.Count > 0);

            _migrator.MigrateTo(0);
        }

        [TearDown]
        public void TearDown()
        {
            _migrator.MigrateTo(0);
        }

        [Test]
        public void Up()
        {
            _migrator.MigrateToLastVersion();
        }

        [Test]
        public void Down()
        {
            _migrator.MigrateToLastVersion();
            _migrator.MigrateTo(0);
        }
    }
}