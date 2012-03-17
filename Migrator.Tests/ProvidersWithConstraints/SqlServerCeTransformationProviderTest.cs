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
using System.Data;
using Migrator.Framework;
using Migrator.Providers.SqlServerCe;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithConstraints
{
    [TestFixture]
    [Category("SqlServerCe")]
    public class SqlServerCeTransformationProviderTest : TransformationProviderWithConstraintTestBase<SqlServerCeTransformationProvider>
    {
        protected override SqlServerCeTransformationProvider Provider()
        {
          return new SqlServerCeTransformationProvider(ConnectionString); 
        }

        public virtual string ConnectionString
        {
            get { return String.Format("Data Source={0}", "unittest.sdf"); }
        }

        [Test]
        public void AddIndex()
        {
            _provider.AddColumn("TestTwo", "TestIndexed", DbType.Boolean);
            _provider.AddIndex("IX_Test_index", "TestTwo", new[] { "TestIndexed" });
            Assert.IsTrue(_provider.IndexExists("IX_Test_index", "TestTwo"));
        }

        [Test, Ignore("SqlServerCe doesn't support check constraints")]
        public override void CanAddCheckConstraint()
        {
        }

        [Test, Ignore("SqlServerCe doesn't support check constraints")]
        public override void RemoveCheckConstraint()
        {
        }

    }
}