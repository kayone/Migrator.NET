using System.Reflection;
using Migrator.Framework;
using Migrator.Framework.Loggers;
using NUnit.Framework;
using NUnit.Mocks;

namespace Migrator.Tests
{
	[TestFixture]
	public class MigrationLoaderTest
	{
		#region Setup/Teardown

		[SetUp]
		public void SetUp()
		{
			SetUpCurrentVersion(0, false);
		}

		#endregion

		MigrationLoader _migrationLoader;

		void SetUpCurrentVersion(int version, bool assertRollbackIsCalled)
		{
			var providerMock = new DynamicMock(typeof (ITransformationProvider));

			providerMock.SetReturnValue("get_CurrentVersion", version);
			providerMock.SetReturnValue("get_Logger", new Logger(false));
			if (assertRollbackIsCalled)
				providerMock.Expect("Rollback");
			else
				providerMock.ExpectNoCall("Rollback");

			_migrationLoader = new MigrationLoader((ITransformationProvider) providerMock.MockInstance, Assembly.GetExecutingAssembly(), true);
			_migrationLoader.MigrationsTypes.Add(typeof (MigratorTest.FirstMigration));
			_migrationLoader.MigrationsTypes.Add(typeof (MigratorTest.SecondMigration));
			_migrationLoader.MigrationsTypes.Add(typeof (MigratorTest.ThirdMigration));
			_migrationLoader.MigrationsTypes.Add(typeof (MigratorTest.ForthMigration));
			_migrationLoader.MigrationsTypes.Add(typeof (MigratorTest.BadMigration));
			_migrationLoader.MigrationsTypes.Add(typeof (MigratorTest.SixthMigration));
			_migrationLoader.MigrationsTypes.Add(typeof (MigratorTest.NonIgnoredMigration));
		}

		[Test]
		[ExpectedException(typeof (DuplicatedVersionException))]
		public void CheckForDuplicatedVersion()
		{
			_migrationLoader.MigrationsTypes.Add(typeof (MigratorTest.FirstMigration));
			_migrationLoader.CheckForDuplicatedVersion();
		}

		[Test]
		public void LastVersion()
		{
			Assert.AreEqual(7, _migrationLoader.LastVersion);
		}

		[Test]
		public void NullIfNoMigrationForVersion()
		{
			Assert.IsNull(_migrationLoader.GetMigration(99999999));
		}

		[Test]
		public void ZeroIfNoMigrations()
		{
			_migrationLoader.MigrationsTypes.Clear();
			Assert.AreEqual(0, _migrationLoader.LastVersion);
		}
	}
}