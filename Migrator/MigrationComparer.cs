using System;
using System.Collections.Generic;
using Migrator.Framework;

namespace Migrator
{
    /// <summary>
    ///   Comparer of Migration by their version attribute.
    /// </summary>
    public class MigrationTypeComparer : IComparer<Type>
    {
        private readonly bool _ascending = true;

        public MigrationTypeComparer(bool ascending)
        {
            _ascending = ascending;
        }

        #region IComparer<Type> Members

        public int Compare(Type x, Type y)
        {
            MigrationAttribute attribOfX =
                (MigrationAttribute) Attribute.GetCustomAttribute(x, typeof (MigrationAttribute));
            MigrationAttribute attribOfY =
                (MigrationAttribute) Attribute.GetCustomAttribute(y, typeof (MigrationAttribute));

            if (_ascending)
                return attribOfX.Version.CompareTo(attribOfY.Version);
            else
                return attribOfY.Version.CompareTo(attribOfX.Version);
        }

        #endregion
    }
}