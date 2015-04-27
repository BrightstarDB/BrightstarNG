namespace BrightstarDB.Storage
{
    /// <summary>
    /// Provides page management services on top of a block provider.
    /// The IPageManager keeps the free list up to date and return free pages when a new page is requested
    /// </summary>
    public interface IPageManager
    {
        /// <summary>
        /// Get the size of the data buffer on the pages provided by this page manager
        /// </summary>
        uint PageSize { get; }

        /// <summary>
        /// Get the page at the specified offset
        /// </summary>
        /// <param name="pageOffset"></param>
        /// <returns>A <see cref="PageStruct"/> instance wrapping the page data and page number.</returns>
        /// <exception cref="BlockOutOfRangeException">Raised if <paramref name="pageOffset"/> specifies a page location past the end of the page store.</exception>
        PageStruct GetPage(ulong pageOffset);

        /// <summary>
        /// Append a new page to the underlying store. Even if there are free pages available, this method
        /// always creates a new page at the end of the store.
        /// </summary>
        /// <returns>A <see cref="PageStruct"/> instance wrapping the newly created page data and page number.</returns>
        PageStruct NewPage();

        /// <summary>
        /// Mark a page with a modified data buffer
        /// </summary>
        /// <param name="page">The modified page</param>
        void MarkDirty(PageStruct page);

        /// <summary>
        /// Flush any modified pages to the underlying store
        /// </summary>
        void Flush();

        /// <summary>
        /// Finalize all work in this commit
        /// </summary>
        void Commit();

        /// <summary>
        /// Close the page manager releasing any resources it has locked
        /// </summary>
        void Close();
    }
}

