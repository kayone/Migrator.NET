using System;

namespace Migrator
{
    /// <summary>
    ///   Exception thrown when a migration number is not unique.
    /// </summary>
    [Serializable]
    public class DuplicatedVersionException : Exception
    {
        public DuplicatedVersionException(long version)
            : base(String.Format("Migration version #{0} is duplicated", version))
        {
        }
    }
}