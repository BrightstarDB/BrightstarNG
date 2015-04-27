namespace BrightstarDB.Storage.BTree
{
    internal interface INodeCache
    {
        void Add(INode node);
        void Remove(INode node);
        void Clear();
        bool TryGetValue(ulong nodeId, out INode node);
    }
}