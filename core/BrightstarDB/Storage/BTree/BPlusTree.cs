using System;
using System.Collections.Generic;
using System.Linq;
using BrightstarDB.Utils;

namespace BrightstarDB.Storage.BTree
{
    internal class BPlusTree
    {
        private ulong _rootId;
        private readonly BPlusTreeConfiguration _config;
        private readonly IPageManager _pageStore;
        private readonly INodeCache _nodeCache;
        private bool _isDirty;

        public BPlusTreeConfiguration Configuration { get { return _config; } }

        /// <summary>
        /// Creates a new tree in the page store
        /// </summary>
        /// <param name="txnId">The transaction id for the update</param>
        /// <param name="pageStore"></param>
        /// <param name="keySize">The size of the B+ tree's key (in bytes)</param>
        /// <param name="dataSize">The size of the values stored in leaf nodes (in bytes)</param>
        public BPlusTree(ulong txnId, IPageManager pageStore, int keySize = 8, int dataSize = 64) 
        {
            _config = new BPlusTreeConfiguration(pageStore, keySize, dataSize, (int)pageStore.PageSize);
            _pageStore = pageStore;
            var root = MakeLeafNode(txnId);
            _rootId = root.PageId;
            _nodeCache = new WeakReferenceNodeCache();
            _nodeCache.Add(root);
        }

        /// <summary>
        /// Opens an existing tree in the page store
        /// </summary>
        /// <param name="pageStore"></param>
        /// <param name="rootPageId">The page ID of the BTree root node</param>
        /// <param name="keySize"></param>
        /// <param name="dataSize"></param>
        /// <param name="profiler"></param>
        public BPlusTree(IPageManager pageStore, ulong rootPageId, int keySize = 8, int dataSize = 64)
        {
            _config = new BPlusTreeConfiguration(pageStore, keySize, dataSize, (int)pageStore.PageSize);
            _pageStore = pageStore;
            _nodeCache = new WeakReferenceNodeCache();
            var root = GetNode(rootPageId);
            _nodeCache.Add(root);
            _rootId = root.PageId;
        }

        /// <summary>
        /// Get a flag indicating if this tree contains unsaved modifications
        /// </summary>
        public bool IsModified { get { return _isDirty; } }

        /// <summary>
        /// Get the ID of the root node of the tree
        /// </summary>
        public ulong RootId { get { return _rootId; } }

        public INode GetNode(ulong nodeId)
        {
            INode ret;
            if (_nodeCache.TryGetValue(nodeId, out ret))
            {
                return ret;
            }

            var nodePage = _pageStore.GetPage(nodeId);
            var header = BitConverter.ToInt32(nodePage.Data, 0);
            if (header < 0)
            {
                ret = MakeInternalNode(nodePage, ~header);
#if DEBUG_BTREE
                        _config.BTreeDebug("{0}: Loaded INTERNAL node from page {1}. {2}",_config.DebugId, nodePage.Id, ret.ToString());
#endif
            }
            else
            {
                ret = MakeLeafNode(nodePage, header);
#if DEBUG_BTREE
                        _config.BTreeDebug("{0}: Loaded LEAF node from page {1}. {2}", _config.DebugId, nodePage.Id, ret.ToString());
#endif
            }
            _nodeCache.Add(ret);
            return ret;
        }

        public bool Search(byte[] key, byte[] valueBuff)
        {
            INode u = GetNode(_rootId);
            while (u is IInternalNode)
            {
                var internalNode = u as IInternalNode;
                u = GetNode(internalNode.GetChildNodeId(key));
            }
            var l = u as ILeafNode;
            return l.GetValue(key, valueBuff);
        }

        public void Insert(WriteablePageStoreSession session, byte[] key, byte[] value, bool overwrite = false)
        {
            bool splitRoot;
            INode rightNode;
            byte[] rootSplitKey;
            var root = GetNode(_rootId);
            Insert(session, root, key, value, out splitRoot, out rightNode, out rootSplitKey, overwrite);
            if (splitRoot)
            {
                var newRoot = MakeInternalNode(session.NextPage(), rootSplitKey, root.PageId,
                    rightNode.PageId);
                //var newRoot = new InternalNode(_pageStore.Create(), rootSplitKey, _root.PageId, rightNode.PageId,
                //                               _config);
                MarkDirty(root);
                MarkDirty(newRoot);
                _rootId = newRoot.PageId;
#if DEBUG_BTREE
                    _config.BTreeDebug("BPlusTree.Insert: Root node has split. New root ID {0}: {1}",_rootId, newRoot.Dump());
#endif
            }
            else
            {
                // Update root page pointer
                // If the store is a BinaryFilePageStore, then the root page ID shouldn't change.
                // If the store is an AppendOnlyPageSTore, then the root will change if the root 
                // is a leaf node or if a lower level split bubbled up to insert a new key into 
                // the root node.
                _rootId = root.PageId;
#if DEBUG_BTREE
                    _config.BTreeDebug("BPlusTree.Insert: Updated root node id is {0}", _rootId);
#endif
            }
        }

        public void Delete(WriteablePageStoreSession session, byte[] key)
        {
            var root = GetNode(_rootId);
            if (root is ILeafNode)
            {
                (root as ILeafNode).Delete(session, key);
                MarkDirty(root);
                // Update root page pointer - see note in Insert() method above
                _rootId = root.PageId;
            }
            else
            {
                bool underAllocation;
                Delete(session, root as IInternalNode, key, out underAllocation);
                if (root.KeyCount == 0)
                {
                    // Now has only a single child leaf node, which should become the new tree root
                    root = GetNode((root as IInternalNode).GetChildPointer(0));
                    _rootId = root.PageId;
                }
                else
                {
                    // Update root page pointer - see note in Insert() method above
                    _rootId = root.PageId;
                }
            }

        }

        /// <summary>
        /// Enumerates the key-value pairs stored in the BTree starting with <paramref name="fromKey"/>
        /// up to <paramref name="toKey"/> (inclusive)
        /// </summary>
        /// <param name="fromKey">The lowest key to return in the enumeration</param>
        /// <param name="toKey">The highest key to return in the enumeration</param>
        /// <param name="profiler"></param>
        /// <returns>An enumeration of key-value pairs from the BTree</returns>
        public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(byte[] fromKey, byte[] toKey )
        {
                if (fromKey.Compare(toKey) > 1)
                {
                    throw new ArgumentException("Scan can only be performed in increasing order.");
                }
                return Scan(GetNode(_rootId), fromKey, toKey);
        }

        public IEnumerable<KeyValuePair<byte[], byte []>> Scan()
        {
                return Scan(GetNode(_rootId));
        }

        private IEnumerable<KeyValuePair<byte[], byte[]>> Scan(INode node)
        {
            if (node is IInternalNode)
            {
                var internalNode = node as IInternalNode;
                foreach(var childNodeId in internalNode.Scan())
                {
                    foreach(var entry in Scan(GetNode(childNodeId)))
                    {
                        yield return entry;
                    }
                }
            }
            if (node is ILeafNode)
            {
                var leaf = node as ILeafNode;
                foreach(var entry in leaf.Scan())
                {
                    yield return entry;
                }
            }
        }

        private IEnumerable<KeyValuePair<byte[], byte[]>> Scan(INode node, byte[] fromKey, byte[] toKey)
        {
            if (node is IInternalNode)
            {
                var internalNode = node as IInternalNode;
                foreach(var childNodeId in internalNode.Scan(fromKey, toKey))
                {
                    foreach(var entry in Scan(GetNode(childNodeId), fromKey, toKey))
                    {
                        yield return entry;
                    }
                }
            }
            else if (node is ILeafNode)
            {
                var leaf = node as ILeafNode;
                foreach(var entry in leaf.Scan(fromKey, toKey))
                {
                    yield return entry;
                }
            }
        }

        /// <summary>
        /// Convenience method that wraps <see cref="Scan(byte[], byte[], BrightstarProfiler)"/> to convert keys to/from ulongs
        /// </summary>
        /// <param name="fromKey">The lowest key to return in the enumeration</param>
        /// <param name="toKey">The highest key to return in the enumeration</param>
        /// <param name="profiler"></param>
        /// <returns>An enumeration of key-value pars from the BTree</returns>
        public IEnumerable<KeyValuePair<ulong, byte[]>> Scan(ulong fromKey, ulong toKey)
        {
            return
                Scan(BitConverter.GetBytes(fromKey), BitConverter.GetBytes(toKey)).Select(
                    v => new KeyValuePair<ulong, byte[]>(BitConverter.ToUInt64(v.Key, 0), v.Value));
        }

        private void Delete(WriteablePageStoreSession session, IInternalNode parentInternalNode, byte[] key, out bool underAllocation)
        {
            if (parentInternalNode.RightmostKey == null)
            {
                throw new ArgumentException("Parent node right key is null");
            }
            var childNodeId = parentInternalNode.GetChildNodeId(key);
            var childNode = GetNode(childNodeId);
            if (childNode is ILeafNode)
            {
                var childLeafNode = childNode as ILeafNode;
                // Delete the key and mark the node as updated. This may update the child node id
                childLeafNode.Delete(session, key);
                MarkDirty(childLeafNode);
                if (childLeafNode.PageId != childNodeId)
                {
                    parentInternalNode.UpdateChildPointer(session, childNodeId, childLeafNode.PageId);
                    childNodeId = childLeafNode.PageId;
                }

                if (childLeafNode.NeedsJoin)
                {
                    ulong leftSiblingId, rightSiblingId;
                    ILeafNode leftSibling = null, rightSibling = null;
                    bool hasLeftSibling = parentInternalNode.GetLeftSibling(childNodeId, out leftSiblingId);
                    if (hasLeftSibling)
                    {
                        leftSibling = GetNode(leftSiblingId) as ILeafNode;
                        if (childLeafNode.RedistributeFromLeft(session, leftSibling))
                        {
                            parentInternalNode.SetLeftKey(session, childLeafNode.PageId, childLeafNode.LeftmostKey);
                            MarkDirty(parentInternalNode);
                            MarkDirty(leftSibling);
                            parentInternalNode.UpdateChildPointer(session, leftSiblingId, leftSibling.PageId);
                            underAllocation = false;
                            return;
                        }
                    }
                    bool hasRightSibling = parentInternalNode.GetRightSiblingId(childNodeId, out rightSiblingId);
                    
                        
                    if (hasRightSibling)
                    {
                        rightSibling = GetNode(rightSiblingId) as ILeafNode;
#if DEBUG
                        if (rightSibling.LeftmostKey.Compare(childLeafNode.RightmostKey) <= 0)
                        {
                            throw new Exception("Right-hand sibling has a left key lower than this nodes right key.");
                        }
#endif
                        if (childLeafNode.RedistributeFromRight(session, rightSibling))
                        {
                            MarkDirty(rightSibling);
                            parentInternalNode.UpdateChildPointer(session, rightSiblingId, rightSibling.PageId);
                            parentInternalNode.SetLeftKey(session, rightSibling.PageId, rightSibling.LeftmostKey);
                            MarkDirty(parentInternalNode);
                            underAllocation = false;
                            return;
                        }
                    }
                    if (hasLeftSibling && childLeafNode.Merge(session, leftSibling))
                    {
                        parentInternalNode.RemoveChildPointer(session, leftSiblingId);
                        parentInternalNode.SetLeftKey(session, childLeafNode.PageId, childLeafNode.LeftmostKey);
                        MarkDirty(parentInternalNode);
                        underAllocation = parentInternalNode.NeedJoin;
                        return;
                    }
                    if (hasRightSibling && childLeafNode.Merge(session, rightSibling))
                    {
                        byte[] nodeKey = parentInternalNode.RemoveChildPointer(session, rightSiblingId);
                        if (nodeKey == null)
                        {
                            // We merged in the right-most node, so we need to generate a key
                            nodeKey = new byte[_config.KeySize];
                            Array.Copy(rightSibling.RightmostKey, nodeKey, _config.KeySize);
                            ByteArrayHelper.Increment(nodeKey);
                        }
                        parentInternalNode.SetKey(session, childLeafNode.PageId, nodeKey);
                        MarkDirty(parentInternalNode);
                        underAllocation = parentInternalNode.NeedJoin;
                        return;
                    }
                }
                underAllocation = false;
                return;
            }


            if (childNode is IInternalNode)
            {
                bool childUnderAllocated;
                var childInternalNode = childNode as IInternalNode;
                Delete(session, childInternalNode, key, out childUnderAllocated);
                if (childInternalNode.PageId != childNodeId)
                {
                    // Child node page changed
                    parentInternalNode.UpdateChildPointer(session, childNodeId, childInternalNode.PageId);
                    MarkDirty(parentInternalNode);
                    childNodeId = childInternalNode.PageId;
                }

                if (childUnderAllocated)
                {
                    IInternalNode leftSibling = null, rightSibling = null;
                    ulong leftSiblingId, rightSiblingId;

                    // Redistribute values from left-hand sibling
                    bool hasLeftSibling = parentInternalNode.GetLeftSibling(childNodeId, out leftSiblingId);
                    if (hasLeftSibling)
                    {
                        leftSibling = GetNode(leftSiblingId) as IInternalNode;
                        byte[] joinKey = parentInternalNode.GetKey(leftSiblingId);
                        var newJoinKey = new byte[_config.KeySize];
                        if (childInternalNode.RedistributeFromLeft(session, leftSibling, joinKey, newJoinKey))
                        {
                            MarkDirty(leftSibling);
                            parentInternalNode.UpdateChildPointer(session, leftSiblingId, leftSibling.PageId);
                            parentInternalNode.SetKey(session, leftSibling.PageId, newJoinKey);
                            MarkDirty(parentInternalNode);
                            underAllocation = false;
                            return;
                        }
                    }

                    // Redistribute values from right-hand sibling
                    bool hasRightSibling = parentInternalNode.GetRightSiblingId(childNodeId, out rightSiblingId);
                    if (hasRightSibling)
                    {
                        rightSibling = GetNode(rightSiblingId) as IInternalNode;
                        byte[] joinKey = parentInternalNode.GetKey(childInternalNode.PageId);
                        byte[] newJoinKey = new byte[_config.KeySize];
                        if (childInternalNode.RedistributeFromRight(session, rightSibling, joinKey, newJoinKey))
                        {
                            MarkDirty(rightSibling);
                            parentInternalNode.UpdateChildPointer(session, rightSiblingId, rightSibling.PageId);
                            // parentInternalNode.SetKey(rightSibling.PageId, newJoinKey); -- think this is wrong should be:
                            parentInternalNode.SetKey(session, childInternalNode.PageId, newJoinKey);
                            MarkDirty(parentInternalNode);
                            underAllocation = false;
                            return;
                        }
                    }

                    // Merge with left-hand sibling
                    if (hasLeftSibling)
                    {
                        // Attempt to merge child node into its left sibling
                        var joinKey = parentInternalNode.GetKey(leftSibling.PageId);
                        var mergedNodeKey = parentInternalNode.GetKey(childInternalNode.PageId);
                        if (mergedNodeKey == null)
                        {
                            mergedNodeKey = new byte[_config.KeySize];
                            Array.Copy(childInternalNode.RightmostKey, mergedNodeKey, _config.KeySize);
                            ByteArrayHelper.Increment(mergedNodeKey);
                        }
                        if (leftSibling.Merge(session, childInternalNode, joinKey))
                        {
                            MarkDirty(leftSibling);
                            if (leftSibling.PageId != leftSiblingId)
                            {
                                // We have a new page id (append-only stores will do this)
                                parentInternalNode.UpdateChildPointer(session, leftSiblingId, leftSibling.PageId);
                            }
                            parentInternalNode.RemoveChildPointer(session, childInternalNode.PageId);
                            parentInternalNode.SetKey(session, leftSibling.PageId, mergedNodeKey);
                            MarkDirty(parentInternalNode);
                            underAllocation = parentInternalNode.NeedJoin;
                            return;
                        }
                    }

                    // Merge with right-hand sibling
                    if (hasRightSibling)
                    {
                        // Attempt to merge right sibling into child node
                        var joinKey = parentInternalNode.GetKey(childNodeId);
                        if (childInternalNode.Merge(session, rightSibling, joinKey))
                        {
                            MarkDirty(childInternalNode);
                            var nodeKey = parentInternalNode.RemoveChildPointer(session, rightSiblingId);
                            if (childInternalNode.PageId != childNodeId)
                            {
                                // We have a new page id for the child node (append-only stores will do this)
                                parentInternalNode.UpdateChildPointer(session, childNodeId, childInternalNode.PageId);
                            }
                            if (nodeKey == null)
                            {
                                // We merged in the right-most node, so we need to generate a key
                                nodeKey = new byte[_config.KeySize];
                                Array.Copy(rightSibling.RightmostKey, nodeKey, _config.KeySize);
                                ByteArrayHelper.Increment(nodeKey);
                            }
                            parentInternalNode.SetKey(session, childInternalNode.PageId, nodeKey);
                            MarkDirty(parentInternalNode);
                            underAllocation = parentInternalNode.NeedJoin;
                            return;
                        }
                    }

                    throw new NotImplementedException(
                        "Not yet implemented handling for internal node becoming under allocated");
                }
                underAllocation = false;
            }
            else
            {
                throw new BrightstarInternalException(String.Format("Unrecognised B+ Tree node class : {0}",
                                                                    childNode.GetType()));
            }
        }


        private ulong Insert(WriteablePageStoreSession session, INode node, byte[] key, byte[] value, out bool split, out INode rightNode,
            out byte[] splitKey, bool overwrite)
        {
            if (node is ILeafNode)
            {
#if DEBUG_BTREE
                _config.BTreeDebug("BPlusTree.Insert Key={0} into LEAF node {1}", key.Dump(), node.PageId);
#endif
                var leaf = node as ILeafNode;
                if (leaf.IsFull)
                {
#if DEBUG_BTREE
                    _config.BTreeDebug("BPlusTree.Insert. Target leaf node is full.");
#endif
                    var newPage = session.NextPage();
                    var newNode = leaf.Split(session, newPage, out splitKey);
                    if (key.Compare(splitKey) < 0)
                    {
                        leaf.Insert(session, key, value, overwrite: overwrite);
                    }
                    else
                    {
                        newNode.Insert(session, key, value, overwrite: overwrite);
                    }
                    MarkDirty(leaf);
                    MarkDirty(newNode);
                    split = true;
                    rightNode = newNode;
                }
                else
                {
                    leaf.Insert(session, key, value, overwrite: overwrite);
                    MarkDirty(leaf);
                    split = false;
                    rightNode = null;
                    splitKey = null;
                }
                return leaf.PageId;
            }
            else
            {
#if DEBUG_BTREE
                _config.BTreeDebug("BPlusTree.Insert Key={0} into INTERNAL node {1}", key.Dump(), node.PageId);
#endif
                var internalNode = node as IInternalNode;
                var childNodeId = internalNode.GetChildNodeId(key);
                var childNode = GetNode(childNodeId);
                bool childSplit;
                INode rightChild;
                byte[] childSplitKey;
                var newChildNodeId = Insert(session, childNode, key, value, out childSplit, out rightChild,
                    out childSplitKey, overwrite);
                if (childSplit)
                {
                    if (internalNode.IsFull)
                    {
#if DEBUG_BTREE
                        _config.BTreeDebug("BPlusTree.Insert: Root node is full.");
#endif
                        // Need to split this node to insert the new child node
                        rightNode = internalNode.Split(session, session.NextPage(), out splitKey);
                        MarkDirty(rightNode);
                        split = true;
                        if (childSplitKey.Compare(splitKey) < 0)
                        {
                            internalNode.Insert(session, childSplitKey, rightChild.PageId);
                        }
                        else
                        {
                            (rightNode as IInternalNode).Insert(session, childSplitKey, rightChild.PageId);
                        }
                        // update child pointers if required (need to check both internalNode and rightNode as we don't know which side the modified child node ended up on)
                        if (newChildNodeId != childNodeId)
                        {
                            internalNode.UpdateChildPointer(session, childNodeId, newChildNodeId);
                            (rightNode as IInternalNode).UpdateChildPointer(session, childNodeId, newChildNodeId);
                        }
                    }
                    else
                    {
                        split = false;
                        rightNode = null;
                        splitKey = null;
                        internalNode.Insert(session, childSplitKey, rightChild.PageId);
                    }
                    if (newChildNodeId != childNodeId)
                    {
                        internalNode.UpdateChildPointer(session, childNodeId, newChildNodeId);
                    }
                    MarkDirty(internalNode);
                    return internalNode.PageId;
                }
                else
                {
                    if (newChildNodeId != childNodeId)
                    {
                        internalNode.UpdateChildPointer(session, childNodeId, newChildNodeId);
                        MarkDirty(internalNode);
                    }
                    split = false;
                    rightNode = null;
                    splitKey = null;
                    return internalNode.PageId;
                }
            }
        }

        private void MarkDirty(INode node)
        {
            _isDirty = true;
            _pageStore.MarkDirty(node.PageId);
        }

        public virtual ulong Save(ulong transactionId)
        {
            _isDirty = false;
            _nodeCache.Clear();
            return RootId;
        }

        public void DumpStructure()
        {
            GetNode(_rootId).DumpStructure(this, 0);
        }

        public void Insert(WriteablePageStoreSession session, ulong key, byte[] value, bool overwrite = false)
        {
            Insert(session, BitConverter.GetBytes(key), value, overwrite);
        }

        public bool Search(ulong key, byte[] valueBuffer)
        {
            return Search(BitConverter.GetBytes(key), valueBuffer);
        }

        public void Delete(WriteablePageStoreSession session, ulong key)
        {
            Delete(session, BitConverter.GetBytes(key));
        }

        public int PreloadTree(int numPages)
        {
            if (numPages == 0) return 0;
            var pageQueue = new Queue<ulong>(numPages);
            pageQueue.Enqueue(_rootId);

            var numLoaded = 0;
            while (numLoaded < numPages)
            {
                ulong pageId;
                try
                {
                    pageId = pageQueue.Dequeue();
                }
                catch (InvalidOperationException)
                {
                    // Raised when the queue is empty
                    return numLoaded;
                }
                var node = GetNode(pageId);
                numLoaded++;
                if (node is IInternalNode)
                {
                    var internalNode = node as IInternalNode;
                    foreach (var childPageId in internalNode.Scan())
                    {
                        pageQueue.Enqueue(childPageId);
                    }
                }
            }
            return numLoaded;
        }

        

        #region Node factory methods

        private ILeafNode MakeLeafNode(ulong txnId)
        {
            return new LeafNode((_pageStore as WriteablePageStoreSession).NextPage(), 0, 0, _config);
        }

        private ILeafNode MakeLeafNode(PageStruct nodePage, int keyCount)
        {
            return new LeafNode(nodePage, keyCount, _config);
        }

        private INode MakeInternalNode(PageStruct nodePage, int keyCount)
        {
            return new InternalNode(nodePage, keyCount, _config);
        }

        private INode MakeInternalNode(PageStruct nodePage, byte[] rootSplitKey, ulong leftPageId, ulong rightPageId)
        {
            return new InternalNode(nodePage, rootSplitKey, leftPageId, rightPageId, _config);
        }

        #endregion
    }

    internal class BrightstarInternalException : Exception
    {
        public BrightstarInternalException(string format)
        {
            throw new NotImplementedException();
        }
    }
}
