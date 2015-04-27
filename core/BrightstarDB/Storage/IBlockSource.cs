using System;

namespace BrightstarDB.Storage
{
    public interface IBlockSource : IDisposable
    {
        /// <summary>
        /// Get the length of the source in blocks
        /// </summary>
        ulong Length { get; }

        /// <summary>
        /// Get the size in bytes of an individual block in this block source
        /// </summary>
        uint BlockSize { get; }

        /// <summary>
        /// Retrieve the data in the block at the specified zero-based offset from the start of the source
        /// </summary>
        /// <param name="blockOffset"></param>
        /// <returns></returns>
        byte[] GetBlock(ulong blockOffset);

        /// <summary>
        /// Mark the block at the specified offset as dirty
        /// </summary>
        void MarkDirty(ulong blockOffset);

        /// <summary>
        /// Return true if the block at the specified offset is dirty, false otherwise
        /// </summary>
        /// <param name="blockOffset"></param>
        /// <returns></returns>
        bool IsDirty(ulong blockOffset);

        /// <summary>
        /// Append a new (empty) block to the end of the source and return its offset
        /// </summary>
        /// <returns></returns>
        ulong Grow();

        /// <summary>
        /// Set the length of the block source to the specified number of blocks
        /// </summary>
        /// <param name="numBlocks"></param>
        void Truncate(ulong numBlocks);

        /// <summary>
        /// Flush any pending updates to the block source
        /// </summary>
        void Flush();

        /// <summary>
        /// Release any resources used by the block source
        /// </summary>
        void Close();
    }
}
