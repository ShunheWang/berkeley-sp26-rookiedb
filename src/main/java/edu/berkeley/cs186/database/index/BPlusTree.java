package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.table.RecordId;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * A persistent B+ tree.
 *
 *   BPlusTree tree = new BPlusTree(bufferManager, metadata, lockContext);
 *
 *   // Insert some values into the tree.
 *   tree.put(new IntDataBox(0), new RecordId(0, (short) 0));
 *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
 *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
 *
 *   // Get some values out of the tree.
 *   tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 *   tree.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 *   tree.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 *   tree.get(new IntDataBox(3)); // Optional.empty();
 *
 *   // Iterate over the record ids in the tree.
 *   tree.scanEqual(new IntDataBox(2));        // [(2, 2)]
 *   tree.scanAll();                             // [(0, 0), (1, 1), (2, 2)]
 *   tree.scanGreaterEqual(new IntDataBox(1)); // [(1, 1), (2, 2)]
 *
 *   // Remove some elements from the tree.
 *   tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 *   tree.remove(new IntDataBox(0));
 *   tree.get(new IntDataBox(0)); // Optional.empty()
 *
 *   // Load the tree (same as creating a new tree).
 *   BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, lockContext);
 *
 *   // All the values are still there.
 *   fromDisk.get(new IntDataBox(0)); // Optional.empty()
 *   fromDisk.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 *   fromDisk.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 */
public class BPlusTree {
    // Buffer manager
    private BufferManager bufferManager;

    // B+ tree metadata
    private BPlusTreeMetadata metadata;

    // root of the B+ tree
    private BPlusNode root;

    // lock context for the B+ tree
    private LockContext lockContext;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Construct a new B+ tree with metadata `metadata` and lock context `lockContext`.
     * `metadata` contains information about the order, partition number,
     * root page number, and type of keys.
     *
     * If the specified order is so large that a single node cannot fit on a
     * single page, then a BPlusTree exception is thrown. If you want to have
     * maximally full B+ tree nodes, then use the BPlusTree.maxOrder function
     * to get the appropriate order.
     *
     * We additionally write a row to the _metadata.indices table with metadata about
     * the B+ tree:
     *
     *   - the name of the tree (table associated with it and column it indexes)
     *   - the key schema of the tree,
     *   - the order of the tree,
     *   - the partition number of the tree,
     *   - the page number of the root of the tree.
     *
     * All pages allocated on the given partition are serializations of inner and leaf nodes.
     */
    public BPlusTree(BufferManager bufferManager, BPlusTreeMetadata metadata, LockContext lockContext) {
        // Prevent child locks - we only lock the entire tree as a whole.
        lockContext.disableChildLocks();
        // By default we want to read the whole tree
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // Sanity checks.
        if (metadata.getOrder() < 0) {
            String msg = String.format(
                    "You cannot construct a B+ tree with negative order %d.",
                    metadata.getOrder());
            throw new BPlusTreeException(msg);
        }

        int maxOrder = BPlusTree.maxOrder(BufferManager.EFFECTIVE_PAGE_SIZE, metadata.getKeySchema());
        if (metadata.getOrder() > maxOrder) {
            String msg = String.format(
                    "You cannot construct a B+ tree with order %d greater than the " +
                            "max order %d.",
                    metadata.getOrder(), maxOrder);
            throw new BPlusTreeException(msg);
        }

        this.bufferManager = bufferManager;
        this.lockContext = lockContext;
        this.metadata = metadata;

        if (this.metadata.getRootPageNum() != DiskSpaceManager.INVALID_PAGE_NUM) {
            this.root = BPlusNode.fromBytes(this.metadata, bufferManager, lockContext,
                    this.metadata.getRootPageNum());
        } else {
            // We're creating the root, which means we need exclusive access
            // on the tree
            LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);
            // Construct the root.
            List<DataBox> keys = new ArrayList<>();
            List<RecordId> rids = new ArrayList<>();
            Optional<Long> rightSibling = Optional.empty();
            this.updateRoot(new LeafNode(this.metadata, bufferManager, keys, rids, rightSibling, lockContext));
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    /**
     * Returns the value associated with `key`.
     *
     *   // Insert a single value into the tree.
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(0, (short) 0);
     *   tree.put(key, rid);
     *
     *   // Get the value we put and also try to get a value we never put.
     *   tree.get(key);                 // Optional.of(rid)
     *   tree.get(new IntDataBox(100)); // Optional.empty()
     */
    public Optional<RecordId> get(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        LeafNode leafNode = root.get(key);
        return leafNode.getKey(key);
    }

    /**
     * scanEqual(k) is equivalent to get(k) except that it returns an iterator
     * instead of an Optional. That is, if get(k) returns Optional.empty(),
     * then scanEqual(k) returns an empty iterator. If get(k) returns
     * Optional.of(rid) for some rid, then scanEqual(k) returns an iterator
     * over rid.
     */
    public Iterator<RecordId> scanEqual(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        Optional<RecordId> rid = get(key);
        if (rid.isPresent()) {
            ArrayList<RecordId> l = new ArrayList<>();
            l.add(rid.get());
            return l.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    /**
     * Returns an iterator over all the RecordIds stored in the B+ tree in
     * ascending order of their corresponding keys.
     *
     *   // Create a B+ tree and insert some values into it.
     *   BPlusTree tree = new BPlusTree("t.txt", Type.intType(), 4);
     *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
     *   tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
     *   tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
     *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
     *   tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
     *
     *   Iterator<RecordId> iter = tree.scanAll();
     *   iter.next(); // RecordId(1, 1)
     *   iter.next(); // RecordId(2, 2)
     *   iter.next(); // RecordId(3, 3)
     *   iter.next(); // RecordId(4, 4)
     *   iter.next(); // RecordId(5, 5)
     *   iter.next(); // NoSuchElementException
     *
     * Note that you CAN NOT materialize all record ids in memory and then
     * return an iterator over them. Your iterator must lazily scan over the
     * leaves of the B+ tree. Solutions that materialize all record ids in
     * memory will receive 0 points.
     */
    public Iterator<RecordId> scanAll() {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        LeafNode leafNode = root.getLeftmostLeaf();
        return new BPlusTreeIterator(leafNode);
    }

    /**
     * Returns an iterator over all the RecordIds stored in the B+ tree that
     * are greater than or equal to `key`. RecordIds are returned in ascending
     * of their corresponding keys.
     *
     *   // Insert some values into a tree.
     *   tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
     *   tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
     *   tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
     *   tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
     *   tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
     *
     *   Iterator<RecordId> iter = tree.scanGreaterEqual(new IntDataBox(3));
     *   iter.next(); // RecordId(3, 3)
     *   iter.next(); // RecordId(4, 4)
     *   iter.next(); // RecordId(5, 5)
     *   iter.next(); // NoSuchElementException
     *
     * Note that you CAN NOT materialize all record ids in memory and then
     * return an iterator over them. Your iterator must lazily scan over the
     * leaves of the B+ tree. Solutions that materialize all record ids in
     * memory will receive 0 points.
     */
    public Iterator<RecordId> scanGreaterEqual(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        LeafNode startLeaf = root.get(key);
        if (startLeaf == null || startLeaf.getKeys().isEmpty()) return java.util.Collections.emptyIterator();

        return new BPlusTreeIterator(startLeaf, key);
    }

    /**
     * Inserts a (key, rid) pair into a B+ tree. If the key already exists in
     * the B+ tree, then the pair is not inserted and an exception is raised.
     *
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(42, (short) 42);
     *   tree.put(key, rid); // Success :)
     *   tree.put(key, rid); // BPlusTreeException :(
     */
    public void put(DataBox key, RecordId rid) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        Optional<Pair<DataBox, Long>> splitPair = root.put(key, rid);
        if (splitPair.isPresent()){
            List<DataBox> newKeys = new ArrayList<DataBox>();
            List<Long> newChildren = new ArrayList<Long>();
            newKeys.add(splitPair.get().getFirst());
            newChildren.add(root.getPage().getPageNum());
            newChildren.add(splitPair.get().getSecond());
            InnerNode newRoot = new InnerNode(metadata, bufferManager, newKeys, newChildren, lockContext);
            updateRoot(newRoot);
        }

    }

    /**
     * Bulk loads data into the B+ tree. Tree should be empty and the data
     * iterator should be in sorted order (by the DataBox key field) and
     * contain no duplicates (no error checking is done for this).
     *
     * fillFactor specifies the fill factor for leaves only; inner nodes should
     * be filled up to full and split in half exactly like in put.
     *
     * This method should raise an exception if the tree is not empty at time
     * of bulk loading. Bulk loading is used when creating a new Index, so think
     * about what an "empty" tree should look like. If data does not meet the
     * preconditions (contains duplicates or not in order), the resulting
     * behavior is undefined. Undefined behavior means you can handle these
     * cases however you want (or not at all) and you are not required to
     * write any explicit checks.
     *
     * The behavior of this method should be similar to that of InnerNode's
     * bulkLoad (see comments in BPlusNode.bulkLoad).
     */
    public void bulkLoad(Iterator<Pair<DataBox, RecordId>> data, float fillFactor) {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        // 3. 核心处理：只在根节点需要分裂时处理，且避免重复分裂
        Optional<Pair<DataBox, Long>> splitPair;
        while (data.hasNext()) {
            splitPair = root.bulkLoad(data, fillFactor);
            // 只有当分裂存在，且当前根不是内部节点（或内部节点未填满）时才创建新根
            if (splitPair.isPresent()) {
                DataBox splitKey = splitPair.get().getFirst();
                long newChildPageNum = splitPair.get().getSecond();

                // 避免重复创建根：如果当前根已经包含该分裂键，跳过
                if (root instanceof InnerNode) {
                    InnerNode innerRoot = (InnerNode) root;
                    if (innerRoot.getKeys().contains(splitKey)) {
                        continue;
                    }
                }

                List<DataBox> newRootKeys = new ArrayList<>();
                List<Long> newRootChildren = new ArrayList<>();
                newRootKeys.add(splitKey);
                newRootChildren.add(root.getPage().getPageNum());
                newRootChildren.add(newChildPageNum);

                InnerNode newRoot = new InnerNode(metadata, bufferManager, newRootKeys, newRootChildren, lockContext);
                updateRoot(newRoot);
            }
        }
    }

    /**
     * Deletes a (key, rid) pair from a B+ tree.
     *
     *   DataBox key = new IntDataBox(42);
     *   RecordId rid = new RecordId(42, (short) 42);
     *
     *   tree.put(key, rid);
     *   tree.get(key); // Optional.of(rid)
     *   tree.remove(key);
     *   tree.get(key); // Optional.empty()
     */
    public void remove(DataBox key) {
        typecheck(key);
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        LeafNode leafNode = root.get(key);
        leafNode.remove(key);
        return;
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Returns a sexp representation of this tree. See BPlusNode.toSexp for
     * more information.
     */
    public String toSexp() {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        return root.toSexp();
    }

    /**
     * Debugging large B+ trees is hard. To make it a bit easier, we can print
     * out a B+ tree as a DOT file which we can then convert into a nice
     * picture of the B+ tree. tree.toDot() returns the contents of DOT file
     * which illustrates the B+ tree. The details of the file itself is not at
     * all important, just know that if you call tree.toDot() and save the
     * output to a file called tree.dot, then you can run this command
     *
     *   dot -T pdf tree.dot -o tree.pdf
     *
     * to create a PDF of the tree.
     */
    public String toDot() {
        // TODO(proj4_integration): Update the following line
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);

        List<String> strings = new ArrayList<>();
        strings.add("digraph g {" );
        strings.add("  node [shape=record, height=0.1];");
        strings.add(root.toDot());
        strings.add("}");
        return String.join("\n", strings);
    }

    /**
     * This function is very similar to toDot() except that we write
     * the dot representation of the B+ tree to a dot file and then
     * convert that to a PDF that will be stored in the src directory. Pass in a
     * string with the ".pdf" extension included at the end (ex "tree.pdf").
     */
    public void toDotPDFFile(String filename) {
        String tree_string = toDot();

        // Writing to intermediate dot file
        try {
            java.io.File file = new java.io.File("tree.dot");
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(tree_string);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Running command to convert dot file to PDF
        try {
            Runtime.getRuntime().exec("dot -T pdf tree.dot -o " + filename).waitFor();
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new BPlusTreeException(e.getMessage());
        }
    }

    public BPlusTreeMetadata getMetadata() {
        return this.metadata;
    }

    /**
     * Returns the largest number d such that the serialization of a LeafNode
     * with 2d entries and an InnerNode with 2d keys will fit on a single page.
     */
    public static int maxOrder(short pageSize, Type keySchema) {
        int leafOrder = LeafNode.maxOrder(pageSize, keySchema);
        int innerOrder = InnerNode.maxOrder(pageSize, keySchema);
        return Math.min(leafOrder, innerOrder);
    }

    /** Returns the partition number that the B+ tree resides on. */
    public int getPartNum() {
        return metadata.getPartNum();
    }

    /**
     * Save the new root page number and update the tree's metadata.
     **/
    private void updateRoot(BPlusNode newRoot) {
        this.root = newRoot;

        metadata.setRootPageNum(this.root.getPage().getPageNum());
        metadata.incrementHeight();
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction != null) {
            transaction.updateIndexMetadata(metadata);
        }
    }

    private void typecheck(DataBox key) {
        Type t = metadata.getKeySchema();
        if (!key.type().equals(t)) {
            String msg = String.format("DataBox %s is not of type %s", key, t);
            throw new IllegalArgumentException(msg);
        }
    }

    // Iterator ////////////////////////////////////////////////////////////////
    private class BPlusTreeIterator implements Iterator<RecordId> {
        // 当前遍历的叶子节点 *懒加载, 用完丢弃, 不缓存
        private LeafNode currentLeafNode;
        // 当前叶子节点的rids迭代器
        private Iterator<RecordId> currentRidIterator;
        // 起始key scanGreaterEqual用, scanAll为null
        private DataBox startKey;
        // 标记是否已定位到 scanGreaterEqual 的起始位置
        private boolean startPosFound;

        /**
         * 构造器: 从最左叶子节点开始，懒加载遍历所有叶子节点
         * 核心: 不预加载任何RecordId到内存, 只在hasNext/next时按需获取
         */
        public BPlusTreeIterator(LeafNode startLeaf) {
            this.currentLeafNode = startLeaf;
            // 初始化当前叶子节点的迭代器
            this.currentRidIterator = startLeaf.getRids().iterator();
            this.startKey = null;
            this.startPosFound = true;
        }

        /**
         * 构造器2: 适配scanGreaterEqual, 从≥key的位置开始遍历
         */
        public BPlusTreeIterator(LeafNode startLeaf, DataBox startKey) {
            this.currentLeafNode = startLeaf;
            this.startKey = startKey;
            this.startPosFound = false;
            findStartPosition(); // 处理起始指针逻辑
        }

        // 定位scanGreaterEqual的起始位置（懒加载，仅找索引，不拷贝数据）
        private void findStartPosition() {
            // 特判
            if (currentLeafNode == null) {
                this.currentRidIterator = java.util.Collections.emptyIterator();
                this.startPosFound = true;
                return;
            }
            int startIdx = InnerNode.numLessThan(startKey, currentLeafNode.getKeys());
            this.currentRidIterator = currentLeafNode.getRids().subList(startIdx, currentLeafNode.getRids().size()).iterator();
            this.startPosFound = true;
        }



        @Override
        public boolean hasNext() {
            if (!startPosFound) return false;
            // 1. 当前叶子节点还有未遍历的RecordId 则返回true
            if (currentRidIterator.hasNext()) {
                return true;
            }

            // 2. 当前叶子节点遍历完, 找下一个右兄弟节点 懒加载下一个节点
            Optional<LeafNode> rightSibling = currentLeafNode.getRightSibling();
            // 由于节点内部数据是自增incr且compact, 因此可以通过第一个在判断节点有没有值;
            while (rightSibling.isPresent()) {
                currentLeafNode = rightSibling.get();
                currentRidIterator = currentLeafNode.getRids().iterator();

                // 新节点有数据 返回true, 停止找下一个
                if (currentRidIterator.hasNext()) return true;

                // 新节点也空, 继续找下一个右兄弟
                rightSibling = currentLeafNode.getRightSibling();
            }

            // 3. 所有叶子节点都遍历完
            return false;
        }

        @Override
        public RecordId next() {
            // 先检查是否有下一个元素
            if (!hasNext()) throw new NoSuchElementException("No more RecordIds in B+ tree");
            // 仅返回当前迭代器的下一个元素 *懒加载
            return currentRidIterator.next();
        }
    }
}
