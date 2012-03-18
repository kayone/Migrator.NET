using System;
using System.Collections.Generic;
using Migrator.Framework;
using NUnit.Framework;

namespace Migrator.Tests
{
    [TestFixture]
    public class MigrationTypeComparerTest
    {
        private readonly Type[] _types = {
                                             typeof (Migration1),
                                             typeof (Migration2),
                                             typeof (Migration3)
                                         };

        [Migration(1, Ignore = true)]
        internal class Migration1 : Migration
        {
            public override void Up()
            {
            }

            public override void Down()
            {
            }
        }

        [Migration(2, Ignore = true)]
        internal class Migration2 : Migration
        {
            public override void Up()
            {
            }

            public override void Down()
            {
            }
        }

        [Migration(3, Ignore = true)]
        internal class Migration3 : Migration
        {
            public override void Up()
            {
            }

            public override void Down()
            {
            }
        }

        [Test]
        public void SortAscending()
        {
            var list = new List<Type>();

            list.Add(_types[1]);
            list.Add(_types[0]);
            list.Add(_types[2]);

            list.Sort(new MigrationTypeComparer(true));

            for (int i = 0; i < 3; i++)
            {
                Assert.AreSame(_types[i], list[i]);
            }
        }

        [Test]
        public void SortDescending()
        {
            var list = new List<Type>();

            list.Add(_types[1]);
            list.Add(_types[0]);
            list.Add(_types[2]);

            list.Sort(new MigrationTypeComparer(false));

            for (int i = 0; i < 3; i++)
            {
                Assert.AreSame(_types[2 - i], list[i]);
            }
        }
    }
}