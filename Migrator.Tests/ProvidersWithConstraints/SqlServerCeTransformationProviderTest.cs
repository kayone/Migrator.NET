using Migrator.Providers.SqlServerCe;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithConstraints
{
    [TestFixture]
    [Category("SqlServerCe")]
    public class SqlServerCeTransformationProviderTest :
        TransformationProviderWithConstraintTestBase<SqlServerCeTransformationProvider>
    {
        protected override SqlServerCeTransformationProvider Provider()
        {
            return new SqlServerCeTransformationProvider("Data Source=unittest23.sdf");
        }

        [Test, Ignore("SqlServerCe doesn't support check constraints")]
        public override void AddRemoveCheckConstraint()
        {
        }
    }
}