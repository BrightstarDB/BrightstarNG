using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BrightstarDB.Storage
{
    public class SyncFileBlockSource : IBlockSource
    {
        private Stream _readStream;
        private Stream _writeStream;
        private Dictionary<ulong, WeakReference<byte[]>> _cleanBlocks; 
        private Dictionary<ulong, byte[]> _dirtyBlocks; 

        public SyncFileBlockSource(string filePath, uint blockSize)
        {
            _writeStream = File.Open(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            _readStream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            BlockSize = blockSize;
            _cleanBlocks = new Dictionary<ulong, WeakReference<byte[]>>();
            _dirtyBlocks = new Dictionary<ulong, byte[]>();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public ulong Length { get { return (ulong)_writeStream.Length/BlockSize; } }

        public uint BlockSize { get; private set; }

        public byte[] GetBlock(ulong blockOffset, bool forWriting = false)
        {
            byte[] ret;
            WeakReference<byte[]> weakRef;
            if (_dirtyBlocks.TryGetValue(blockOffset, out ret)) return ret;
            if (_cleanBlocks.TryGetValue(blockOffset, out weakRef) && weakRef.TryGetTarget(out ret)) return ret;
            ret = new byte[BlockSize];
            _readStream.Seek((long)blockOffset, SeekOrigin.Begin);
            _readStream.Read(ret, 0, (int)BlockSize);
            if (forWriting)
            {
                _dirtyBlocks[blockOffset] = ret;
            }
            else
            {
                _cleanBlocks[blockOffset] = new WeakReference<byte[]>(ret);
            }
            return ret;
        }

        public void MarkDirty(ulong blockOffset)
        {
            byte[] buff;
            WeakReference<byte[]> weakRef;
            if (_cleanBlocks.TryGetValue(blockOffset, out weakRef) && weakRef.TryGetTarget(out buff))
            {
                _dirtyBlocks[blockOffset] = buff;
                _cleanBlocks.Remove(blockOffset);
            }
            if (_dirtyBlocks.ContainsKey(blockOffset)) return;
            buff = GetBlock(blockOffset);
            _cleanBlocks.Remove(blockOffset);
            _dirtyBlocks[blockOffset] = buff;
        }

        public bool IsDirty(ulong blockOffset)
        {
            return _dirtyBlocks.ContainsKey(blockOffset);
        }

        public ulong Grow()
        {
            var offset = (ulong)_writeStream.Length/BlockSize;
            _writeStream.SetLength(_writeStream.Length + BlockSize);
            _dirtyBlocks[offset] = new byte[BlockSize];
            return offset;
        }

        public void Truncate(ulong numBlocks)
        {
            var truncatedLength = numBlocks*BlockSize;
            _writeStream.SetLength((long)truncatedLength);
            foreach (var k in _dirtyBlocks.Keys.Where(x => x >= numBlocks).ToArray())
            {
                _dirtyBlocks.Remove(k);
            }
            foreach (var k in _cleanBlocks.Keys.Where(x => x >= numBlocks).ToArray())
            {
                _cleanBlocks.Remove(k);
            }
        }

        public void Flush()
        {
            foreach (var dirtyBlockEntry in _dirtyBlocks)
            {
                _writeStream.Seek((long)(dirtyBlockEntry.Key*BlockSize), SeekOrigin.Begin);
                _writeStream.Write(dirtyBlockEntry.Value, 0, (int)BlockSize);
                _writeStream.Flush();
            }
        }

        public void Close()
        {
            _writeStream.Flush();
            _writeStream.Close();
            _readStream.Close();
        }
    }
}
