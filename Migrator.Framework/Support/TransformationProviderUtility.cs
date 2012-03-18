namespace Migrator.Framework.Support
{
    public static class TransformationProviderUtility
    {
        public const int MaxLengthForForeignKeyInOracle = 30;
        private static readonly string[] CommonWords = new[] {"Test"};

        public static string CreateForeignKeyName(string tableName, string foreignKeyTableName)
        {
            string fkName = string.Format("FK_{0}_{1}", tableName, foreignKeyTableName);

            return AdjustNameToSize(fkName, MaxLengthForForeignKeyInOracle, true);
        }

        public static string AdjustNameToSize(string name, int totalCharacters, bool removeCommmonWords)
        {
            string adjustedName = name;

            if (adjustedName.Length > totalCharacters)
            {
                if (removeCommmonWords)
                {
                    adjustedName = RemoveCommonWords(adjustedName);
                }
            }

            if (adjustedName.Length > totalCharacters) adjustedName = adjustedName.Substring(0, totalCharacters);

            if (name != adjustedName)
            {
                //log.WarnFormat("Name has been truncated from: {0} to: {1}", name, adjustedName);
            }

            return adjustedName;
        }

        private static string RemoveCommonWords(string adjustedName)
        {
            foreach (var word in CommonWords)
            {
                if (adjustedName.Contains(word))
                {
                    adjustedName = adjustedName.Replace(word, string.Empty);
                }
            }
            return adjustedName;
        }

        public static string FormatTableName(string schema, string tableName)
        {
            return string.IsNullOrEmpty(schema) ? tableName : string.Format("{0}.{1}", schema, tableName);
        }
    }
}