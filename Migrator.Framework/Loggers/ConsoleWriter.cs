using System;

namespace Migrator.Framework.Loggers
{
    public class ConsoleWriter : ILogWriter
    {
        #region ILogWriter Members

        public void Write(string message, params object[] args)
        {
            Console.Write(message, args);
        }

        public void WriteLine(string message, params object[] args)
        {
            Console.WriteLine(message, args);
        }

        #endregion
    }
}