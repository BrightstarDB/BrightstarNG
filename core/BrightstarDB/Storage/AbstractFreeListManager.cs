namespace BrightstarDB.Storage
{
    public abstract class AbstractFreeListManager
    {
        protected IPageManager PageManager { get; private set; }

        /// <summary>
        /// Create a new free list manager instance that manages the free pages for the provided IPageManager instance
        /// </summary>
        /// <param name="pageManager"></param>
        protected AbstractFreeListManager(IPageManager pageManager)
        {
            PageManager = pageManager;
        }

        /// <summary>
        /// Read a persistent free list from the page store starting at the specified page
        /// </summary>
        /// <param name="rootPageOffset"></param>
        public abstract void Load(ulong rootPageOffset);

        /// <summary>
        /// Record a page as being freed by a specific commit
        /// </summary>
        /// <param name="pageNumber"></param>
        /// <param name="commitNumber"></param>
        public abstract void AddFreePage(ulong pageNumber, ulong commitNumber);

        /// <summary>
        /// Release the pages freed by the specified commit for reuse
        /// </summary>
        /// <param name="commitNumber"></param>
        public abstract void UnlockCommit(ulong commitNumber);

        /// <summary>
        /// Record a new read reference on the specified commit
        /// </summary>
        /// <param name="commitNumber"></param>
        public abstract void IncrementRefCount(ulong commitNumber);

        /// <summary>
        /// Remove a read reference on the specified commit
        /// </summary>
        /// <param name="commitNumber"></param>
        public abstract void DecrementRefCount(ulong commitNumber);

        /// <summary>
        /// Return the offset of the next available free page without removing it from the free page list
        /// </summary>
        /// <returns></returns>
        public abstract ulong? PeekFree();

        /// <summary>
        /// Return the offset of the next available free page and remove it from the free page list
        /// </summary>
        /// <returns></returns>
        public abstract ulong? PopFree();

        /// <summary>
        /// Persiste the free page list to the underlying storage
        /// </summary>
        /// <returns>The page number for the first free list page or 0ul if no free list was written</returns>
        public abstract ulong Commit();
    }
}
