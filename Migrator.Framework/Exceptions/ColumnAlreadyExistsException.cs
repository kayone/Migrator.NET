using System;

namespace Migrator.Framework.Exceptions
{
    public class ColumnAlreadyExistsException : Exception
    {
        public ColumnAlreadyExistsException(string table, string column)
            : base(string.Format("Column with the same name '{0}' already exists in table '{1}'", column, table))
        {
        }
    }
}