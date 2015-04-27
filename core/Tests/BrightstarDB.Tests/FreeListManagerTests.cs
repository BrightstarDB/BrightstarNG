using System;
using BrightstarDB.Storage;
using NUnit.Framework;

namespace BrightstarDB.Tests
{
    [TestFixture]
    public class FreeListManagerTests
    {
        private IBlockSource _blockSource;
        private AbstractFreeListManager _flManager;
        private IPageManager _pageManager;

        [SetUp]
        public void TestSetUp()
        {
            _blockSource = new MemoryBlockSource(4096);
            _pageManager = new SimplePageManager(_blockSource);
            _flManager = new DefaultFreeListManager(_pageManager);
            // Create 2000 pages to play with
            for (int i = 0; i < 2000; i++)
            {
                _pageManager.NewPage();
            }
        }

        [Test]
        public void TestLoadShortFreeList()
        {
            // Create a freelist root page with 10 free pages recorded on it
            var page = _pageManager.GetPage(1);
            BitConverter.GetBytes(0ul).CopyTo(page.Data, 0);
            BitConverter.GetBytes(10ul).CopyTo(page.Data, 4);
            for (int i = 0; i < 10; i++)
            {
                BitConverter.GetBytes((ulong)(2+i)).CopyTo(page.Data, (i+2)*4);
            }
            _flManager.Load(1);

            for (int i = 0; i < 10; i++)
            {
                var freePageRef = _flManager.PopFree();
                Assert.That(freePageRef.HasValue);
                Assert.That(freePageRef.Value, Is.EqualTo((ulong)(i+2)));
            }
            Assert.That(_flManager.PopFree().HasValue, Is.False);
        }

        [Test]
        public void TestLoadLongFreeList()
        {
            // Create a linked pair of free list pages
            var startPage = _pageManager.GetPage(1);
            var linkedPage = _pageManager.GetPage(2);
            BitConverter.GetBytes(2ul).CopyTo(startPage.Data, 0);
            BitConverter.GetBytes(10ul).CopyTo(startPage.Data, 4);
            BitConverter.GetBytes(0ul).CopyTo(linkedPage.Data, 0);
            BitConverter.GetBytes(10ul).CopyTo(linkedPage.Data, 4);
            for (int i = 0; i < 10; i++)
            {
                BitConverter.GetBytes((ulong)(3+i)).CopyTo(startPage.Data, (i+2)*4);
                BitConverter.GetBytes((ulong)(13+i)).CopyTo(linkedPage.Data, (i+2)*4);
            }

            // Load the free list
            _flManager.Load(1);

            // Expect 20 free pages from page 3 to 22 (inclusive)
            for (int i = 0; i < 20; i++)
            {
                var freePageRef = _flManager.PopFree();
                Assert.That(freePageRef.HasValue, "No free page after {0} pops", i);
                Assert.That(freePageRef.Value, Is.EqualTo(3+i));
            }
            // And then there should be no more free pages
            Assert.That(_flManager.PopFree().HasValue, Is.False);
        }

        [Test]
        public void TestAddedFreePageNotAvailableWhileCommitIsLocked()
        {
            _flManager.AddFreePage(10, 1);
            Assert.That(_flManager.PeekFree().HasValue, Is.False);
            _flManager.UnlockCommit(1);
            Assert.That(_flManager.PeekFree().HasValue);
            Assert.That(_flManager.PopFree().Value, Is.EqualTo(10));
        }

        [Test]
        public void TestAddedFreePageNotAvailableWhileCommitIsReferenced()
        {
            _flManager.AddFreePage(10, 1);
            _flManager.IncrementRefCount(1);
            _flManager.UnlockCommit(1);
            Assert.That(_flManager.PeekFree().HasValue, Is.False);
            _flManager.DecrementRefCount(1);
            Assert.That(_flManager.PeekFree().HasValue);
            Assert.That(_flManager.PopFree().Value, Is.EqualTo(10));
        }

        [Test]
        public void TestAddedFreePageGetsPersistedOnCommit()
        {
            _flManager.AddFreePage(10, 1);
            var rootPageOffset = _flManager.Commit();
            var rootPage = _pageManager.GetPage(rootPageOffset);

            var fwdLink = BitConverter.ToUInt32(rootPage.Data, 0);
            var freeCount = BitConverter.ToUInt32(rootPage.Data, 4);
            var freePageId = BitConverter.ToUInt32(rootPage.Data, 8);
            Assert.That(fwdLink, Is.EqualTo(0ul));
            Assert.That(freeCount, Is.EqualTo(1ul));
            Assert.That(freePageId, Is.EqualTo(10ul));
        }

    }
}
