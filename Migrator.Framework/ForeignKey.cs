namespace Migrator.Framework
{
    public class ForeignKey
    {
        public ForeignKey(string name, string foreignTable, string[] foreignColumns, string primaryTable,
                          string[] primaryColumns,
                          ForeignKeyConstraintType constraintType = ForeignKeyConstraintType.NoAction)
        {
            Name = name;
            ForeignTable = foreignTable;
            ForeignColumns = foreignColumns;
            PrimaryTable = primaryTable;
            PrimaryColumns = primaryColumns;
            ConstraintType = constraintType;
        }

        public ForeignKey(string name, string foreignTable, string foreignColumn, string primaryTable,
                          string primaryColumn,
                          ForeignKeyConstraintType constraintType = ForeignKeyConstraintType.NoAction)
            : this(name, foreignTable, new[] {foreignColumn}, primaryTable, new[] {primaryColumn}, constraintType)
        {
        }

        public ForeignKey(string foreignTable, string[] foreignColumns, string primaryTable, string[] primaryColumns,
                          ForeignKeyConstraintType constraintType = ForeignKeyConstraintType.NoAction)
        {
            Name = string.Format("FK_{0}_{1}", primaryTable, foreignTable);
            ForeignTable = foreignTable;
            ForeignColumns = foreignColumns;
            PrimaryTable = primaryTable;
            PrimaryColumns = primaryColumns;
            ConstraintType = constraintType;
        }

        public ForeignKey(string foreignTable, string foreignColumn, string primaryTable, string primaryColumn,
                          ForeignKeyConstraintType constraintType = ForeignKeyConstraintType.NoAction)
            : this(foreignTable, new[] {foreignColumn}, primaryTable, new[] {primaryColumn}, constraintType)
        {
        }


        public string Name { get; private set; }

        public string ForeignTable { get; set; }

        public string[] ForeignColumns { get; private set; }

        public string PrimaryTable { get; set; }

        public string[] PrimaryColumns { get; private set; }

        public ForeignKeyConstraintType ConstraintType { get; private set; }
    }
}