using System;

namespace BrightstarDB.Storage
{
    public class SimplePageManager : IPageManager
    {
        private readonly IBlockSource _blockSource;

        public SimplePageManager(IBlockSource blockSource)
        {
            _blockSource = blockSource;
        }

        public uint PageSize
        {
            get { return _blockSource.BlockSize; }
        }

        public PageStruct GetPage(ulong pageOffset)
        {
            return new PageStruct {PageNumber = pageOffset, Data = _blockSource.GetBlock(pageOffset)};
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
            // Ensure the underlying block source is flushed
            _blockSource.Flush();
        }

        public void Close()
        {
            _blockSource.Close();
        }
    }

}
