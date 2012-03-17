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
using System.Data;
using Migrator.Framework;
using Migrator.Providers.Mysql;
using NUnit.Framework;

namespace Migrator.Tests.ProvidersWithConstraints
{
    [TestFixture]
    [Category("MySql")]
    public class MySqlTransformationProviderTest : TransformationProviderWithConstraintTestBase<MySqlTransformationProvider>
    {
        protected override MySqlTransformationProvider Provider()
        {
            return new MySqlTransformationProvider(ConnectionString);
        }

        public virtual string ConnectionString
        {
            get { return string.Format("Server=localhost;Database=test;Uid=unittest;Pwd=unittest"); }
        }

        // [Test,Ignore("MySql doesn't support check constraints")]
        public override void CanAddCheckConstraint()
        {
        }

        [Test]
        public void AddTableWithMyISAMEngine()
        {
            _provider.AddTable("Test", 
                               new Column("Id", DbType.Int32, ColumnProperty.NotNull),
                               new Column("name", DbType.String, 50)
                );
        }
    }
}