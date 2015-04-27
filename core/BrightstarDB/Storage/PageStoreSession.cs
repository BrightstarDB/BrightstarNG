using System;

namespace BrightstarDB.Storage
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
}
