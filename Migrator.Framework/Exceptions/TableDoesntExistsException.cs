using System;

namespace Migrator.Framework.Exceptions
{
    public class TableDoesntExistsException : Exception
    {
        public TableDoesntExistsException(string table)
            : base(string.Format("Table with the name '{0}' doesn't exist in database", table))
        {
        }
    }
}