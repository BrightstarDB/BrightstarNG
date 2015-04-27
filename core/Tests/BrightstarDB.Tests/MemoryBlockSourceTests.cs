using System.Collections.Generic;
using BrightstarDB.Storage;
using NUnit.Framework;

namespace BrightstarDB.Tests
{
    [TestFixture]
    public class MemoryBlockSourceTests
    {
        [Test]
        public void SimpleWriteAndReadTest()
        {
            using (var bs = new MemoryBlockSource(4096))
            {
                var pageId = bs.Grow();
                Assert.That(pageId, Is.EqualTo(0));
                SetBytes(bs.GetBlock(pageId, true), 1, 0, 4096);

            }
        }

        [Test]
        public void TestWriteMultiplePages()
        {
            using (var bs = new MemoryBlockSource(4096))
            {
                for (var i = 0; i < 256; i++)
                {
                    var pageId = bs.Grow();
                    Assert.That(pageId, Is.EqualTo(i));
                    SetBytes(bs.GetBlock(pageId, true), (byte)i, 0, 4096);
                    bs.MarkDirty(pageId);
                }
                Assert.That(bs.Length, Is.EqualTo(256));

                for (ulong i = 0; i < 256; i++)
                {
                    Assert.That(bs.IsDirty(i));
                    var blk = bs.GetBlock(i, false);
                    for (var j = 0; j < 4096; j++)
                    {
                        Assert.That(blk[j], Is.EqualTo(i));
                    }
                }
            }
        }

        [Test]
        public void TestTruncate()
        {
            using (var bs = new MemoryBlockSource(4096))
            {
                for (var i = 0; i < 256; i++)
                {
                    var pageId = bs.Grow();
                    SetBytes(bs.GetBlock(pageId, true), (byte)i, 0, 4096);
                }
                bs.Truncate(128);
                Assert.That(bs.Length, Is.EqualTo(128));
                var nextPage = bs.Grow();
                Assert.That(nextPage, Is.EqualTo(128));
            }
        }

        [Test]
        public void TestFlushClearsDirtyMarks()
        {
            using (var bs = new MemoryBlockSource(4096))
            {
                for (var i = 0; i < 256; i++)
                {
                    var pageId = bs.Grow();
                    Assert.That(pageId, Is.EqualTo(i));
                    SetBytes(bs.GetBlock(pageId, true), (byte)i, 0, 4096);
                    bs.MarkDirty(pageId);
                }
                Assert.That(bs.Length, Is.EqualTo(256));
                bs.Flush();

                for (ulong i = 0; i < 256; i++)
                {
                    Assert.That(bs.IsDirty(i), Is.False);
                    var blk = bs.GetBlock(i, false);
                    for (var j = 0; j < 4096; j++)
                    {
                        Assert.That(blk[j], Is.EqualTo(i));
                    }
                }
            }
        }

        private static void SetBytes(IList<byte> buff, byte value, int start, int count)
        {
            for (var i = 0; i < count; i++)
            {
                buff[start + i] = value;
            }
        }
    }
}
