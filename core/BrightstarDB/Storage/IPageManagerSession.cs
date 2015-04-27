namespace BrightstarDB.Storage
{
    public interface IPageManagerSession
    {
        /// <summary>
        /// Get the commit number that the session is currently reading from or writing to
        /// </summary>
        ulong CommitId { get; }

        /// <summary>
        /// Get a boolean flag indicating if this is a readonly session (true) or a read/write session (false)
        /// </summary>
        bool IsReadOnly { get; set; }

        /// <summary>
        /// Retrieve the content of the page at the specified offset
        /// </summary>
        /// <param name="pageOffset"></param>
        /// <returns></returns>
        PageStruct GetPage(ulong pageOffset);

    }

    public interface IWriteablePageManagerSession : IPageManagerSession
    {
        /// <summary>
        /// Get the next available page for writing to. This may return 
        /// a page from the free list
        /// </summary>
        /// <returns></returns>
        PageStruct NextPage();

        /// <summary>
        /// Append a new page to the store and return it. This will not
        /// return a page from the free list
        /// </summary>
        /// <returns></returns>
        PageStruct NewPage();

        /// <summary>
        /// Create a shadow copy of the page at the specified offset and
        /// return the shadow copy page. The shadow copy may be created using
        /// a page from the free list
        /// </summary>
        /// <param name="pageOffset"></param>
        /// <returns></returns>
        PageStruct ShadowPage(ulong pageOffset);

        /// <summary>
        /// Mark a modified page as dirty so that its contents will be written out
        /// on the next call to <see cref="Flush()"/> or <see cref="Commit()"/>
        /// </summary>
        /// <param name="pageOffset"></param>
        void MarkDirty(ulong pageOffset);

        /// <summary>
        /// Flush modified pages to the underlying block source but keep the session
        /// open for further changes
        /// </summary>
        void Flush();

        /// <summary>
        /// Commit all remaining pages to the underlying block source and update the 
        /// persistent free list. This closes the session for further updates.
        /// </summary>
        /// <returns>The offset of the root page of the persistent free list or 0 if 
        /// there are no free pages.</returns>
        ulong Commit();
    }
}
