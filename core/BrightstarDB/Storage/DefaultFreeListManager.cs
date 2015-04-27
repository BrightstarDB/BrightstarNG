using System;
using System.Collections.Generic;
using System.Linq;

namespace BrightstarDB.Storage
{
    public class DefaultFreeListManager : AbstractFreeListManager
    {
        private readonly LinkedList<ulong> _availablePages;
        private readonly Dictionary<ulong, CommitFreeList> _commitFreeLists;
        private readonly List<ulong> _reservedPages; 

        public DefaultFreeListManager(IPageManager pageManager) : base(pageManager)
        {
            _availablePages = new LinkedList<ulong>();
            _commitFreeLists = new Dictionary<ulong, CommitFreeList>();
            _reservedPages = new List<ulong>();
        }

        public override void Load(ulong rootPageOffset)
        {
            lock (_commitFreeLists)
            {
                var currentPage = PageManager.GetPage(rootPageOffset, false);
                while (true)
                {
                    _reservedPages.Add(currentPage.PageNumber);
                    var nextPageOffset = BitConverter.ToUInt32(currentPage.Data, 0);
                    var numEntries = (int) BitConverter.ToUInt32(currentPage.Data, 4);
                    for (var i = 0; i < numEntries; i++)
                    {
                        _availablePages.AddLast(BitConverter.ToUInt32(currentPage.Data, (i + 2)*4));
                    }
                    if (nextPageOffset == 0) break;
                    currentPage = PageManager.GetPage(nextPageOffset, false);
                }
            }
        }

        public override void AddFreePage(ulong pageNumber, ulong commitNumber)
        {
            lock (_commitFreeLists)
            {
                GetCommitFreeList(commitNumber).AddPage(pageNumber);
            }
        }

        public override void UnlockCommit(ulong commitNumber)
        {
            lock (_commitFreeLists)
            {
                CommitFreeList freeList;
                if (_commitFreeLists.TryGetValue(commitNumber, out freeList))
                {
                    freeList.IsLocked = false;
                    if (freeList.RefCount == 0)
                    {
                        foreach (var p in freeList.FreedPages)
                        {
                            _availablePages.AddLast(p);
                        }
                    }
                }
            }
        }

        public override void IncrementRefCount(ulong commitNumber)
        {
            lock (_commitFreeLists)
            {
                GetCommitFreeList(commitNumber).RefCount++;
            }
        }

        public override void DecrementRefCount(ulong commitNumber)
        {
            lock (_commitFreeLists)
            {
                CommitFreeList freeList;
                if (!_commitFreeLists.TryGetValue(commitNumber, out freeList)) return;
                freeList.RefCount--;
                if (freeList.RefCount > 0) return;
                foreach (var p in freeList.FreedPages)
                {
                    _availablePages.AddLast(p);
                }
            }
        }


        public override ulong? PeekFree()
        {
            try
            {
                return _availablePages.First.Value;
            }
            catch (Exception)
            {
                return null;
            }
        }

        public override ulong? PopFree()
        {
            lock (_availablePages)
            {
                try
                {
                    var ret = _availablePages.First.Value;
                    _availablePages.RemoveFirst();
                    return ret;
                }
                catch (Exception)
                {
                    return null;
                }
            }
        }

        public override ulong Commit()
        {
            var maxEntryCount = (PageManager.PageSize/4) - 2;
            var ix = 0;
            var buff = new byte[PageManager.PageSize];
            var freeListPages = new List<ulong>();
            lock (_availablePages)
            {
                lock (_commitFreeLists)
                {
                    var freeList = _availablePages.Union(_commitFreeLists.Values.SelectMany(l => l.FreedPages)).ToList();
                    var pagesRequired = freeList.Count/maxEntryCount;
                    if (freeList.Count%maxEntryCount > 0) pagesRequired++;
                    while (pagesRequired < _reservedPages.Count)
                    {
                        var removedPage = _reservedPages.Last();
                        _reservedPages.RemoveAt(_reservedPages.Count - 1);
                        freeList.Add(removedPage);
                    }
                    PageStruct? prevPage = null;
                    foreach (var pageId in freeList)
                    {
                        BitConverter.GetBytes(pageId).CopyTo(buff, (ix+2)*4);
                        ix++;
                        if (ix == maxEntryCount)
                        {
                            BitConverter.GetBytes(ix).CopyTo(buff, 4);
                            var flPage = GetNextFreelistPage();
                            freeListPages.Add(flPage.PageNumber);
                            buff.CopyTo(flPage.Data, 0);
                            if (prevPage != null)
                            {
                               BitConverter.GetBytes(flPage.PageNumber).CopyTo(prevPage.Value.Data, 0);
                            }
                            prevPage = flPage;
                        }
                    }

                    if (ix > 0)
                    {
                        BitConverter.GetBytes(ix).CopyTo(buff, 4);
                        var flPage = GetNextFreelistPage();
                        freeListPages.Add(flPage.PageNumber);
                        buff.CopyTo(flPage.Data, 0);
                        if (prevPage != null)
                        {
                            BitConverter.GetBytes(flPage.PageNumber).CopyTo(prevPage.Value.Data, 0);
                        }
                    }
                }
            }
            _reservedPages.AddRange(freeListPages);
            return freeListPages.Count > 0 ? freeListPages[0] : 0;
        }

        private PageStruct GetNextFreelistPage()
        {
            if (_reservedPages.Count > 0)
            {
                var ret = PageManager.GetPage(_reservedPages[0], true);
                _reservedPages.RemoveAt(0);
                return ret;
            }
            // Use NewPage rather than NextPage as we don't want to modify the free list at this stage.
            return PageManager.NewPage();
        }


        private CommitFreeList GetCommitFreeList(ulong commitNumber)
        {
            CommitFreeList freeList;
            if (_commitFreeLists.TryGetValue(commitNumber, out freeList))
            {
                return freeList;
            }
            freeList = new CommitFreeList(commitNumber);
            _commitFreeLists[commitNumber] = freeList;
            return freeList;
        }

    }

    internal class CommitFreeList
    {
        public ulong CommitId { get; private set; }
        public  HashSet<ulong> FreedPages { get; private set; } 

        public bool IsLocked { get; set; }
        public uint RefCount { get; set; }

        public CommitFreeList(ulong commitId)
        {
            CommitId = commitId;
            FreedPages = new HashSet<ulong>();
            IsLocked = true;
        }

        public void AddPage(ulong pageId)
        {
            FreedPages.Add(pageId);
        }
    }
}
