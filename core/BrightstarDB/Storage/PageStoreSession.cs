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

        public virtual PageStruct GetPage(ulong pageId, bool forWriting)
        {
            if (forWriting) throw new ArgumentException("Cannot use a read-only session to open a page for writing");
            return PageManager.GetPage(pageId, false);
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
