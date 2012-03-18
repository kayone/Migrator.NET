using Migrator.Providers.Mysql;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithConstraints
{
    [TestFixture]
    [Category("MySql")]
    public class MySqlTransformationProviderTest :
        TransformationProviderWithConstraintTestBase<MySqlTransformationProvider>
    {
        protected override MySqlTransformationProvider Provider()
        {
            return new MySqlTransformationProvider(ConnectionString);
        }

        private string ConnectionString
        {
            get { return string.Format("Server=localhost;Uid=unittests;Pwd=unittests"); }
        }

        [Test, Ignore("MySql doesn't support check constraints")]
        public override void AddRemoveCheckConstraint()
        {
        }

        [Test, Ignore("Not implemented yet")]
        public override void AddRemoveForeignKey()
        {
            base.AddRemoveForeignKey();
        }

        [Test, Ignore("Not implemented yet")]
        public override void RemoveUnexistingForeignKey()
        {
            base.RemoveUnexistingForeignKey();
        }
    }
}