using System;

namespace BrightstarDB.Storage
{
    public class StorageException : Exception
    {
    }

    public class BlockOutOfRangeException : StorageException{}
}
