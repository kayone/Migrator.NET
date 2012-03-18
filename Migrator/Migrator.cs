using System;
using System.Collections.Generic;
using System.Reflection;
using Migrator.Framework;
using Migrator.Framework.Loggers;

namespace Migrator
{
    /// <summary>
    ///   Migrations mediator.
    /// </summary>
    public class Migrator
    {
        private readonly ILogger _logger = new Logger(false);
        private readonly MigrationLoader _migrationLoader;
        private readonly TransformationProviderBase _provider;

        protected bool _dryrun;


        public Migrator(TransformationProviderBase provider, Assembly migrationAssembly = null)
        {
            if (migrationAssembly == null)
                migrationAssembly = Assembly.GetCallingAssembly();

            _provider = provider;

            _migrationLoader = new MigrationLoader(provider, migrationAssembly);
            _migrationLoader.CheckForDuplicatedVersion();
        }

        /// <summary>
        ///   Returns registered migration <see cref="System.Type">types</see> .
        /// </summary>
        public List<Type> MigrationsTypes
        {
            get { return _migrationLoader.MigrationsTypes; }
        }

        /// <summary>
        ///   Returns the current migrations applied to the database.
        /// </summary>
        public List<long> AppliedMigrations
        {
            get { return _provider.AppliedMigrations; }
        }

        public virtual bool DryRun
        {
            get { return _dryrun; }
            set { _dryrun = value; }
        }

        /// <summary>
        ///   Run all migrations up to the latest. Make no changes to database if dryrun is true.
        /// </summary>
        public void MigrateToLastVersion()
        {
            MigrateTo(_migrationLoader.LastVersion);
        }

        /// <summary>
        ///   Migrate the database to a specific version. Runs all migration between the actual version and the specified version. If <c>version</c> is greater then the current version, the <c>Up()</c> method will be invoked. If <c>version</c> lower then the current version, the <c>Down()</c> method of previous migration will be invoked. If <c>dryrun</c> is set, don't write any changes to the database.
        /// </summary>
        /// <param name="version"> The version that must became the current one </param>
        public void MigrateTo(long version)
        {
            if (_migrationLoader.MigrationsTypes.Count == 0)
            {
                _logger.Warn("No public classes with the Migration attribute were found.");
                return;
            }

            bool firstRun = true;
            BaseMigrate migrate = BaseMigrate.GetInstance(_migrationLoader.GetAvailableMigrations(), _provider, _logger);
            migrate.DryRun = DryRun;
            //Logger.Started(migrate.AppliedVersions, version);

            while (migrate.Continue(version))
            {
                IMigration migration = _migrationLoader.GetMigration(migrate.Current);
                if (null == migration)
                {
                    _logger.Skipping(migrate.Current);
                    migrate.Iterate();
                    continue;
                }

                try
                {
                    migrate.Migrate(migration);
                }
                catch (Exception ex)
                {
                    //Logger.Exception(migrate.Current, migration.Name, ex);

                    // Oho! error! We rollback changes.
                    //Logger.RollingBack(migrate.Previous);
                    _provider.Rollback();

                    throw;
                }

                migrate.Iterate();
            }

            //Logger.Finished(migrate.AppliedVersions, version);
        }
    }
}