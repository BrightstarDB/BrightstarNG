using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BrightstarDB.Storage;
using BrightstarDB.Storage.BTree;
using NUnit.Framework;

namespace BrightstarDB.Tests
{
    [TestFixture]
    public class BTreeTests
    {
        [Test]
        public void TestSimpleInsert()
        {
            var blockSource = new MemoryBlockSource(4096);
            var pageManager = new SimplePageManager(blockSource);
            var freeListManager = new DefaultFreeListManager(pageManager);
            ulong treeRoot;
            ulong freePageRoot;
            using (var s = new WriteablePageStoreSession(0, 1, pageManager, freeListManager))
            {
                var btree = new BPlusTree(s, pageManager, 8, 8);
                btree.Insert(s, 1, BitConverter.GetBytes(123ul));
                treeRoot = btree.RootId;
                freePageRoot = s.Commit();
            }

            Assert.That(treeRoot, Is.EqualTo(0));
            Assert.That(freePageRoot, Is.EqualTo(0));

            byte[] buff = new byte[8];
            using (var s = new PageStoreSession(1, pageManager, freeListManager))
            {
                var btree = new BPlusTree(pageManager, treeRoot, 8, 8);
                Assert.That(btree.Search(1ul, buff));
                Assert.That(BitConverter.ToUInt32(buff, 0), Is.EqualTo(123ul));
                s.Close();
            }

            using (var s = new WriteablePageStoreSession(1, 2, pageManager, freeListManager))
            {
                var btree = new BPlusTree(pageManager, treeRoot, 8, 8);
                btree.Insert(s, 2, BitConverter.GetBytes(234ul));
                treeRoot = btree.RootId;
                freePageRoot = s.Commit();
            }
            freeListManager.UnlockCommit(1ul);

            Assert.That(treeRoot, Is.EqualTo(1));
            Assert.That(freePageRoot, Is.EqualTo(2));

            using (var s = new WriteablePageStoreSession(2, 3, pageManager, freeListManager))
            {
                var btree = new BPlusTree(pageManager, treeRoot, 8, 8);
                btree.Insert(s, 3, BitConverter.GetBytes(345ul));
                treeRoot = btree.RootId;
                freePageRoot = s.Commit();
            }
            freeListManager.UnlockCommit(2ul);

            Assert.That(treeRoot, Is.EqualTo(3));
            Assert.That(freePageRoot, Is.EqualTo(2)); // Should reuse the same root page

            using (var s = new WriteablePageStoreSession(3, 4, pageManager, freeListManager))
            {
                var btree = new BPlusTree(pageManager, treeRoot, 8, 8);
                btree.Insert(s, 4, BitConverter.GetBytes(456ul));
                treeRoot = btree.Save(4);
                freePageRoot = s.Commit();
            }
            freeListManager.UnlockCommit(3ul);

            // Page 0 was released when commit 2 got unlocked so it should be used as the new tree root page
            Assert.That(treeRoot, Is.EqualTo(0));
            Assert.That(freePageRoot, Is.EqualTo(2));
        }
    }
}
