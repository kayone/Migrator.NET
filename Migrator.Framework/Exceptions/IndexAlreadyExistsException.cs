using System;

namespace Migrator.Framework.Exceptions
{
    public class IndexAlreadyExistsException : Exception
    {
        public IndexAlreadyExistsException(string table, string indexName)
            : base(string.Format("Index with the same name '{0}' already exists in table '{1}'", indexName, table))
        {
        }
    }
}