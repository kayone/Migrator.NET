using System;
using System.IO;
using AutoMoq;
using Moq;
using NUnit.Framework;

namespace Kayone.TestFoundation
{
    public abstract class TestCore<TSubject> : LoggingTest
    {
        private AutoMoqer _mocker;
        protected AutoMoqer Mocker
        {
            get
            {
                if (_mocker == null)
                {
                    _mocker = new AutoMoqer();
                }

                return _mocker;
            }
        }
        
        [SetUp]
        public void TestCoreSetup()
        {

        }

        [TearDown]
        public void TestCoreTearDown()
        {
            _mocker = null;
        }


        protected virtual TSubject Subject
        {
            get
            {
                return Mocker.Resolve<TSubject>();
            }
        }

        protected void WithStrictMocker()
        {
            if (_mocker != null)
                throw new InvalidOperationException("Can not switch to a strict container after container has been used. make sure this is the first call in your test.");

            _mocker = new AutoMoqer(MockBehavior.Strict);
        }

        public static string GetLongString(int lenght)
        {
            return new String('x', lenght);
        }

        public static string TempFolder
        {
            get { return Path.Combine(Directory.GetCurrentDirectory(), "temp"); }
        }
    }
}