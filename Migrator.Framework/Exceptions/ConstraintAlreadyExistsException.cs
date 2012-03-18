using System;

namespace Migrator.Framework.Exceptions
{
    public class ConstraintAlreadyExistsException : Exception
    {
        public ConstraintAlreadyExistsException(string table, string constraintName)
            : base(
                string.Format("Constraint with the same name '{0}' already exists on table '{1}'", constraintName, table)
                )
        {
        }
    }
}