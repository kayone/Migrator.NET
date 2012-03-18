using System;

namespace Migrator.Framework.Exceptions
{
    public class TableAlreadyExistsException : Exception
    {
        public TableAlreadyExistsException(string table)
            : base(string.Format("Table with the same name '{0}' already exists in database", table))
        {
        }
    }
}