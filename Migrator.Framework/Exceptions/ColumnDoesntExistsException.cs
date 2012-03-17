using System;

namespace Migrator.Framework.Exceptions
{
    public class ColumnDoesntExistsException : Exception
    {
        public ColumnDoesntExistsException(string table, string column)
            : base(string.Format("Column with name '{0}' doesn't exists in table '{1}'", column, table))
        {
        }
    }
}