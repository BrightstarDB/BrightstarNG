using System;
using System.Runtime.InteropServices;

namespace BrightstarDB.Storage
{
    public struct PageStruct
    {
        public ulong PageNumber;
        public byte[] Data;
        public bool IsWriteable;
        public bool IsDirty;

        public void SetData(byte[] data, int srcOffset, int destOffset, int len)
        {
            if (!IsWriteable) throw new InvalidOperationException("Attempted to write to a readonly page");
            if (len == 0) return;
            Array.ConstrainedCopy(data, srcOffset, Data, destOffset, len > 0 ? len : data.Length);
            IsDirty = true;
        }

    }
}