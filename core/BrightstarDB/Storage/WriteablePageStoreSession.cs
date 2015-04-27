namespace BrightstarDB.Storage
{
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
            if (!freePage.HasValue)
            {
                return PageManager.NewPage();
            }
            var ret= GetPage(freePage.Value);
            ret.IsWriteable = true;
            return ret;
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