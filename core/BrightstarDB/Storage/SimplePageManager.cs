using System;

namespace BrightstarDB.Storage
{
    public class SimplePageManager : IPageManager
    {
        private readonly IBlockSource _blockSource;
        private readonly AbstractFreeListManager _freeListManager;

        public SimplePageManager(IBlockSource blockSource)
        {
            _blockSource = blockSource;
            _freeListManager = new DefaultFreeListManager(this);
        }

        public uint PageSize
        {
            get { return _blockSource.BlockSize; }
        }

        public PageStruct GetPage(ulong pageOffset)
        {
            return new PageStruct {PageNumber = pageOffset, Data = _blockSource.GetBlock(pageOffset)};
        }

        public PageStruct CopyPage(ulong pageOffset)
        {
            var srcBlock = _blockSource.GetBlock(pageOffset);
            var newPage = NewPage();
            Array.Copy(srcBlock, newPage.Data, PageSize);
            return newPage;
        }

        public PageStruct NextPage()
        {
            ulong? nextFreeOffset = _freeListManager.PopFree();
            return nextFreeOffset.HasValue ? GetPage(nextFreeOffset.Value) : NewPage();
        }

        public PageStruct NewPage()
        {
            var newPageOffset = _blockSource.Grow();
            var newBlockData = _blockSource.GetBlock(newPageOffset);
            var newPage  = new PageStruct {PageNumber = newPageOffset, Data = newBlockData};
            return newPage;
        }

        public void MarkDirty(PageStruct page)
        {
            _blockSource.MarkDirty(page.PageNumber);
        }

        public void Flush()
        {
            _blockSource.Flush();
        }

        public void Commit()
        {
            // Ensure the free list is updated
            _freeListManager.Commit();
            // Ensure the underlying block source is flushed
            // TODO: This may be ensured by the commit on the free list?
            _blockSource.Flush();
        }

        public void Close()
        {
            _blockSource.Close();
        }
    }

}
