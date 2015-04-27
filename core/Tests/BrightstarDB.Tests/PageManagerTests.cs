﻿using System;
using System.Linq;
using System.Management.Instrumentation;
using BrightstarDB.Storage;
using NUnit.Framework;

namespace BrightstarDB.Tests
{
    [TestFixture]
    public class PageManagerTests
    {
        private IBlockSource _blockSource;
        private IPageManager _pageManager;
        private readonly byte[] _magicString = {0x01, 0x02, 0x03, 0x04, 0x05};

        [SetUp]
        public void TestSetUp()
        {
            _blockSource = new MemoryBlockSource(4096);
            _pageManager = new SimplePageManager(_blockSource);
        }

        [Test]
        public void TestNewPageExtendsBlockSource()
        {
            ulong currentLength = _blockSource.Length;
            var newPage = _pageManager.NewPage();
            Assert.That(newPage.PageNumber == currentLength);
            Assert.That(_blockSource.Length, Is.EqualTo(currentLength+1));
        }

        [Test]
        public void TestGetPageReturnsLatestData()
        {
            var newPage = _pageManager.NewPage();
            Array.Copy(_magicString, newPage.Data, _magicString.Length);

            var getPage = _pageManager.GetPage(newPage.PageNumber, false);
            Assert.AreEqual(_magicString, getPage.Data.Take(5).ToArray());
        }

        [Test]
        [ExpectedException(typeof(BlockOutOfRangeException))]
        public void TestGetPageCannotExceedBounds()
        {
            var p = _pageManager.GetPage(_blockSource.Length, false);
        }

    }
}
