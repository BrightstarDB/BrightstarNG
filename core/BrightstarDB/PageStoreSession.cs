using System;
using BrightstarDB.Storage;

namespace BrightstarDB
{
    public class PageStoreSession : IDisposable
    {
        public ulong ReadCommitId { get; private set; }

        protected IPageManager PageManager;
        protected AbstractFreeListManager FreeListManager;

        private bool _isClosed;
        public PageStoreSession(ulong commitId, IPageManager pageManager, AbstractFreeListManager freeListManager)
        {
            PageManager = pageManager;
            FreeListManager = freeListManager;
            FreeListManager.IncrementRefCount(commitId);
            ReadCommitId = commitId;
        }

        public PageStruct GetPage(ulong pageId)
        {
            return PageManager.GetPage(pageId);
        }

        public void Close()
        {
            if (!_isClosed)
            {
                FreeListManager.DecrementRefCount(ReadCommitId);
                _isClosed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
        }
    }

    public class WriteablePageStoreSession : PageStoreSession
    {
        public ulong WriteCommitId { get; private set; }

        public WriteablePageStoreSession(ulong readCommitId, ulong writeCommitId, IPageManager pageManager,
            AbstractFreeListManager freeListManager) :
                base(readCommitId, pageManager, freeListManager)
        {
            WriteCommitId = writeCommitId;
        }

        public PageStruct CopyPage(ulong pageId)
        {
            var srcPage = GetPage(pageId);
            var destPage = NextPage();
            srcPage.Data.CopyTo(destPage.Data, 0);
            FreeListManager.AddFreePage(pageId, WriteCommitId);
            return destPage;
        }

        public void FreePage(ulong pageId)
        {
            FreeListManager.AddFreePage(pageId, WriteCommitId);
        }

        public PageStruct NextPage()
        {
            var freePage = FreeListManager.PopFree();
            if (freePage.HasValue)
            {
                return GetPage(freePage.Value);
            }
            return PageManager.NewPage();
        }

        public void MarkDirty(PageStruct page)
        {
            PageManager.MarkDirty(page);
        }

        public ulong Commit()
        {
            var freeListRoot = FreeListManager.Commit();
            PageManager.Commit();
            return freeListRoot;
        }

    }
}
