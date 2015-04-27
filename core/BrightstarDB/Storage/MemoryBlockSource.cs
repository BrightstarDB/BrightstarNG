using System;
using System.Collections.Generic;

namespace BrightstarDB.Storage
{
    public class MemoryBlockSource : IBlockSource
    {
        private List<byte[]> _blocks;
        private readonly HashSet<ulong> _dirtyBlocks; 

        public MemoryBlockSource(uint blockSize)
        {
            BlockSize = blockSize;
            _blocks = new List<byte[]>();
            _dirtyBlocks = new HashSet<ulong>();
        }

        public void Dispose()
        {
            Close();
        }

        public ulong Length { get { return (ulong) _blocks.Count; } }

        public uint BlockSize { get; private set; }

        public byte[] GetBlock(ulong blockOffset, bool forWriting)
        {
            try
            {
                return _blocks[(int) blockOffset];
            }
            catch (ArgumentOutOfRangeException)
            {
                throw new BlockOutOfRangeException();
            }
        }

        public void MarkDirty(ulong blockOffset)
        {
            _dirtyBlocks.Add(blockOffset);
        }

        public bool IsDirty(ulong blockOffset)
        {
            return _dirtyBlocks.Contains(blockOffset);
        }

        public ulong Grow()
        {
            _blocks.Add(new byte[BlockSize]);
            return Length - 1;
        }

        public void Truncate(ulong numBlocks)
        {
            _blocks.RemoveRange((int)(Length - numBlocks), (int)numBlocks);
        }

        public void Flush()
        {
            // Nowhere to flush to, just clear the dirty marks
            _dirtyBlocks.Clear();
        }

        public void Close()
        {
            if (_blocks != null)
            {
                _blocks.Clear();
                _blocks = null;
            }
        }


    }

}
