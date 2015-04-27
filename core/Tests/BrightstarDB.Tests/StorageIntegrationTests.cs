using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BrightstarDB.Storage;
using NUnit.Framework;

namespace BrightstarDB.Tests
{
    [TestFixture]
    public class StorageIntegrationTests
    {
        private IBlockSource _blockSource;
        private IPageManager _pageManager;
        private AbstractFreeListManager _freeListManager;
        private readonly byte[] _magicString = {0x01, 0x02, 0x03, 0x04, 0x05};
        [SetUp]
        public void TestSetUp()
        {
            _blockSource = new MemoryBlockSource(4096);
            _pageManager = new SimplePageManager(_blockSource);
            _freeListManager = new DefaultFreeListManager(_pageManager);
        }

        [Test]
        public void TestReusePage()
        {
            // Create a page
            var initSession = new WriteablePageStoreSession(0, 1, _pageManager, _freeListManager);
            var startPage = initSession.NextPage();
            _magicString.CopyTo(startPage.Data, 0);
            initSession.Commit();

            // Create a shadow copy of that page
            var copySession = new WriteablePageStoreSession(1, 2, _pageManager, _freeListManager);
            var copyPage = copySession.CopyPage(startPage.PageNumber);
            copySession.Commit();

            // Release the commit
            _freeListManager.UnlockCommit(2ul);

            // Get the next page
            var nextSession = new WriteablePageStoreSession(2, 3, _pageManager, _freeListManager);
            var nextPage = nextSession.NextPage();
            nextSession.Commit();

            Assert.That(nextPage.PageNumber, Is.EqualTo(startPage.PageNumber));
        }

        [Test]
        public void TestCopyPageCopiesSourceData()
        {
            PageStruct startPage, copyPage;
            // Create a page
            using (var initSession = new WriteablePageStoreSession(0, 1, _pageManager, _freeListManager))
            {
                startPage = initSession.NextPage();
                _magicString.CopyTo(startPage.Data, 0);
                initSession.Commit();
            }

            // Create a shadow copy of that page
            using (var copySession = new WriteablePageStoreSession(1, 2, _pageManager, _freeListManager))
            {
                copyPage = copySession.CopyPage(startPage.PageNumber);
                copySession.Commit();
            }

            Assert.That(startPage.Data.Take(5).ToArray(), Is.EqualTo(_magicString));
            Assert.That(copyPage.Data, Is.EqualTo(startPage.Data));
        }

        [Test]
        [ExpectedException(typeof(BlockOutOfRangeException))]
        public void TestCopyPageCannotExceedBounds()
        {
            using (var initSession = new WriteablePageStoreSession(0, 1, _pageManager, _freeListManager))
            {
                var destPage = initSession.CopyPage(_blockSource.Length);
            }
        }


    }
}
