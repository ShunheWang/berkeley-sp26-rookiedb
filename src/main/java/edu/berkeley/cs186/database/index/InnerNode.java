package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * A inner node of a B+ tree. Every inner node in a B+ tree of order d stores
 * between d and 2d keys. An inner node with n keys stores n + 1 "pointers" to
 * children nodes (where a pointer is just a page number). Moreover, every
 * inner node is serialized and persisted on a single page; see toBytes and
 * fromBytes for details on how an inner node is serialized. For example, here
 * is an illustration of an order 2 inner node:
 *
 *     +----+----+----+----+
 *     | 10 | 20 | 30 |    |
 *     +----+----+----+----+
 *    /     |    |     \
 */
class InnerNode extends BPlusNode {
    // Metadata about the B+ tree that this node belongs to.
    private BPlusTreeMetadata metadata;

    // Buffer manager
    private BufferManager bufferManager;

    // Lock context of the B+ tree
    private LockContext treeContext;

    // The page on which this leaf is serialized.
    private Page page;

    // The keys and child pointers of this inner node. See the comment above
    // LeafNode.keys and LeafNode.rids in LeafNode.java for a warning on the
    // difference between the keys and children here versus the keys and children
    // stored on disk. `keys` is always stored in ascending order.
    private List<DataBox> keys;
    private List<Long> children;

    // Constructors ////////////////////////////////////////////////////////////
    /**
     * Construct a brand new inner node.
     */
    InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
              List<Long> children, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
                keys, children, treeContext);
    }

    /**
     * Construct an inner node that is persisted to page `page`.
     */
    private InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                      List<DataBox> keys, List<Long> children, LockContext treeContext) {
        try {
            assert (keys.size() <= 2 * metadata.getOrder());
            assert (keys.size() + 1 == children.size());

            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.children = new ArrayList<>(children);
            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    // See BPlusNode.get.
    @Override
    public LeafNode get(DataBox key) {
        // 1. 获取key所在的子节点的位置
        int childPtr = numLessThanEqual(key, keys);

        // 2. 根据子节点指针获取key
        BPlusNode nextNode = getChild(childPtr);

        // 3. 递归: 调用子节点
        return nextNode.get(key);
    }

    // See BPlusNode.getLeftmostLeaf.
    @Override
    public LeafNode getLeftmostLeaf() {
        assert(children.size() > 0);
        // 1. 直接取第一个子节点，避免重复调用getChildren()
        BPlusNode currChild = getChild(0);

        // 2. 终止条件：如果是leafNode 则返回
        if (currChild instanceof LeafNode) return (LeafNode) currChild;

        // 3. innerNode 则继续递归
        return currChild.getLeftmostLeaf();
    }

    // See BPlusNode.put.
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // 1. 空校验
        if (key == null || rid == null) throw new IllegalArgumentException("Key/RecordId cannot be null");

        if (keys == null || children == null || children.isEmpty()) throw new IllegalStateException("InnerNode has no children");

        // 2. 获取key对应子节点位置
        int index = numLessThanEqual(key, keys);
        BPlusNode child = getChild(index);
        // 3. 递归调用子节点put (btw, 到达leafNode 调用的实际上是leafNode 的put而不是innerNode 中的put)
        Optional<Pair<DataBox, Long>> pair = child.put(key, rid);

        // 4. 获取[d, 2d]
        int order = metadata.getOrder();
        int maxSize = order * 2;

        // 5. 子节点未分裂：直接返回空
        if (!pair.isPresent()) return Optional.empty();


        // 6. 子节点分裂：取出分隔键和新子节点页号
        DataBox newKey = pair.get().getFirst();
        Long newChildPageId = pair.get().getSecond();

        // 7. 插入分隔键和新子节点（操作类成员变量，避免命名冲突）
        this.keys.add(index, newKey);
        this.children.add(index + 1, newChildPageId);
        // 关键：插入后立即同步 *刷盘
        sync();

        // 8. 当前节点未超容: 返回空
        if (this.keys.size() <= maxSize) return Optional.empty();


        // 8. 当前节点超容：分裂
        // 8.1 计算分裂点：中间键的索引（order）
        int splitPos = order;
        // 8.2 新节点的键：splitPos+1 到末尾（共order个键）
        List<DataBox> newNodeKeys = new ArrayList<>(this.keys.subList(splitPos + 1, this.keys.size()));
        // 8.3 新节点的子节点：splitPos+1 到末尾（共order+1个）
        List<Long> newNodeChildren = new ArrayList<>(this.children.subList(splitPos + 1, this.children.size()));
        // 8.4 向上传播的分隔键（中间键）
        DataBox popUpKey = this.keys.get(splitPos);
        // 8.5 原节点保留前splitPos个键
        this.keys = new ArrayList<>(this.keys.subList(0, splitPos));
        // 8.6 原节点保留前splitPos+1个子节点
        this.children = new ArrayList<>(this.children.subList(0, splitPos + 1));

        // 8.7 创建新内部节点并同步 *刷盘
        InnerNode newNode = new InnerNode(metadata, bufferManager, newNodeKeys, newNodeChildren, treeContext);
        long popUpPageNum = newNode.getPage().getPageNum();
        newNode.sync(); // 新节点同步
        this.sync();    // 原节点同步（分裂后的数据）

        // 8. 返回向上传播的分隔键+新节点页号
        return Optional.of(new Pair<>(popUpKey, popUpPageNum));
    }

    // See BPlusNode.bulkLoad.
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
                                                  float fillFactor) {
        // 1. 校验
        if (data == null) throw new IllegalArgumentException("Data iterator cannot be null");
        if (fillFactor <= 0 || fillFactor > 1.0) throw new IllegalArgumentException("Fill factor must be in (0, 1]");
        if (children == null || children.isEmpty()) throw new IllegalStateException("InnerNode has no children for bulk load");

        // 2. 获取[d, 2d]
        int order = metadata.getOrder();
        int maxKeySize = 2 * order;
        // 3. 批量加载的目标填充数 按fillFactor计算
        int targetFillSize = (int) Math.floor(maxKeySize * fillFactor);

        // 4. 缓存最右侧子节点
        BPlusNode rightMostChild = null;

        // 3. 批量填充子节点：直到达到目标填充率 或 数据耗尽
        while (keys.size() < targetFillSize && data.hasNext()) {
            // 3.1 获取最右侧子节点（缓存复用，减少磁盘页读取）
            rightMostChild = getChild(children.size() - 1); // 最右侧子节点索引是children.size()-1（原代码keys.size()等价）
            // 3.2 递归批量加载子节点
            Optional<Pair<DataBox, Long>> popUpPair = rightMostChild.bulkLoad(data, fillFactor);

            // 3.3 子节点分裂：插入分隔键和新子节点
            if (popUpPair.isPresent()) {
                DataBox splitKey = popUpPair.get().getFirst();
                Long newChildPageNum = popUpPair.get().getSecond();
                // 插入到当前节点末尾（批量加载数据有序，末尾插入天然有序，无需排序）
                this.keys.add(splitKey);
                this.children.add(newChildPageNum);
                // 插入后即时同步，保证数据一致性
                sync();

                // 插入后若超过目标填充率，提前终止循环（避免过度填充）
                if (this.keys.size() >= targetFillSize) {
                    break;
                }
            }
        }

        // 4. 当前节点未达到分裂阈值：返回空
        if (this.keys.size() <= maxKeySize) {
            sync(); // 最终同步一次，确保所有修改落地
            return Optional.empty();
        }

        // 5. 当前节点满：分裂（修复命名冲突+边界漏洞）
        // 5.1 计算分裂点（内部节点分裂规则：中间键向上传播）
        int splitPos = order;
        DataBox popUpKey = this.keys.get(splitPos); // 向上传播的分隔键

        // 5.2 新节点的键：splitPos+1 到末尾（共order-1个键）
        List<DataBox> newNodeKeys = new ArrayList<>(this.keys.subList(splitPos + 1, this.keys.size()));
        // 5.3 新节点的子节点：splitPos+1 到末尾（共order个）
        List<Long> newNodeChildren = new ArrayList<>(this.children.subList(splitPos + 1, this.children.size()));
        // 5.4 原节点保留前splitPos个键和splitPos+1个子节点
        this.keys = new ArrayList<>(this.keys.subList(0, splitPos));
        this.children = new ArrayList<>(this.children.subList(0, splitPos + 1));

        // 5.5 创建新节点并同步
        InnerNode newNode = new InnerNode(metadata, bufferManager, newNodeKeys, newNodeChildren, treeContext);
        long popUpPageNum = newNode.getPage().getPageNum();
        newNode.sync(); // 新节点同步到磁盘
        this.sync();    // 原节点分裂后同步

        // 6. 返回分隔键+新节点页号
        return Optional.of(new Pair<>(popUpKey, popUpPageNum));
    }

    // See BPlusNode.remove.
    @Override
    public void remove(DataBox key) {
        LeafNode leafNode = get(key);
        leafNode.remove(key);
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    private BPlusNode getChild(int i) {
        long pageNum = children.get(i);
        return BPlusNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
    }

    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<Long> getChildren() {
        return children;
    }
    /**
     * Returns the largest number d such that the serialization of an InnerNode
     * with 2d keys will fit on a single page.
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        //   1 + 4 + (n * keySize) + ((n + 1) * 8)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 4 is the number of bytes used to store n,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - 8 is the number of bytes used to store a child pointer.
        //
        // Solving the following equation
        //
        //   5 + (n * keySize) + ((n + 1) * 8) <= pageSizeInBytes
        //
        // we get
        //
        //   n = (pageSizeInBytes - 13) / (keySize + 8)
        //
        // The order d is half of n.
        int keySize = keySchema.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + 8);
        return n / 2;
    }

    /**
     * Given a list ys sorted in ascending order, numLessThanEqual(x, ys) returns
     * the number of elements in ys that are less than or equal to x. For
     * example,
     *
     *   numLessThanEqual(0, Arrays.asList(1, 2, 3, 4, 5)) == 0
     *   numLessThanEqual(1, Arrays.asList(1, 2, 3, 4, 5)) == 1
     *   numLessThanEqual(2, Arrays.asList(1, 2, 3, 4, 5)) == 2
     *   numLessThanEqual(3, Arrays.asList(1, 2, 3, 4, 5)) == 3
     *   numLessThanEqual(4, Arrays.asList(1, 2, 3, 4, 5)) == 4
     *   numLessThanEqual(5, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *   numLessThanEqual(6, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *
     * This helper function is useful when we're navigating down a B+ tree and
     * need to decide which child to visit. For example, imagine an index node
     * with the following 4 keys and 5 children pointers:
     *
     *     +---+---+---+---+
     *     | a | b | c | d |
     *     +---+---+---+---+
     *    /    |   |   |    \
     *   0     1   2   3     4
     *
     * If we're searching the tree for value c, then we need to visit child 3.
     * Not coincidentally, there are also 3 values less than or equal to c (i.e.
     * a, b, c).
     */
    static <T extends Comparable<T>> int numLessThanEqual(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) <= 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    static <T extends Comparable<T>> int numLessThan(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) < 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(children.get(i)).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(children.get(children.size() - 1)).append(")");
        return sb.toString();
    }

    @Override
    public String toSexp() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(getChild(i).toSexp()).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(getChild(children.size() - 1).toSexp()).append(")");
        return sb.toString();
    }

    /**
     * An inner node on page 0 with a single key k and two children on page 1 and
     * 2 is turned into the following DOT fragment:
     *
     *   node0[label = "<f0>|k|<f1>"];
     *   ... // children
     *   "node0":f0 -> "node1";
     *   "node0":f1 -> "node2";
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("<f%d>", i));
            ss.add(keys.get(i).toString());
        }
        ss.add(String.format("<f%d>", keys.size()));

        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        String node = String.format("  node%d[label = \"%s\"];", pageNum, s);

        List<String> lines = new ArrayList<>();
        lines.add(node);
        for (int i = 0; i < children.size(); ++i) {
            BPlusNode child = getChild(i);
            long childPageNum = child.getPage().getPageNum();
            lines.add(child.toDot());
            lines.add(String.format("  \"node%d\":f%d -> \"node%d\";",
                    pageNum, i, childPageNum));
        }

        return String.join("\n", lines);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // When we serialize an inner node, we write:
        //
        //   a. the literal value 0 (1 byte) which indicates that this node is not
        //      a leaf node,
        //   b. the number n (4 bytes) of keys this inner node contains (which is
        //      one fewer than the number of children pointers),
        //   c. the n keys, and
        //   d. the n+1 children pointers.
        //
        // For example, the following bytes:
        //
        //   +----+-------------+----+-------------------------+-------------------------+
        //   | 00 | 00 00 00 01 | 01 | 00 00 00 00 00 00 00 03 | 00 00 00 00 00 00 00 07 |
        //   +----+-------------+----+-------------------------+-------------------------+
        //    \__/ \___________/ \__/ \_________________________________________________/
        //     a         b        c                           d
        //
        // represent an inner node with one key (i.e. 1) and two children pointers
        // (i.e. page 3 and page 7).

        // All sizes are in bytes.
        assert (keys.size() <= 2 * metadata.getOrder());
        assert (keys.size() + 1 == children.size());
        int isLeafSize = 1;
        int numKeysSize = Integer.BYTES;
        int keysSize = metadata.getKeySchema().getSizeInBytes() * keys.size();
        int childrenSize = Long.BYTES * children.size();
        int size = isLeafSize + numKeysSize + keysSize + childrenSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 0);
        buf.putInt(keys.size());
        for (DataBox key : keys) {
            buf.put(key.toBytes());
        }
        for (Long child : children) {
            buf.putLong(child);
        }
        return buf.array();
    }

    /**
     * Loads an inner node from page `pageNum`.
     */
    public static InnerNode fromBytes(BPlusTreeMetadata metadata,
                                      BufferManager bufferManager, LockContext treeContext, long pageNum) {
        Page page = bufferManager.fetchPage(treeContext, pageNum);
        Buffer buf = page.getBuffer();

        byte nodeType = buf.get();
        assert(nodeType == (byte) 0);

        List<DataBox> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();
        int n = buf.getInt();
        for (int i = 0; i < n; ++i) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        for (int i = 0; i < n + 1; ++i) {
            children.add(buf.getLong());
        }
        return new InnerNode(metadata, bufferManager, page, keys, children, treeContext);
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof InnerNode)) {
            return false;
        }
        InnerNode n = (InnerNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
                keys.equals(n.keys) &&
                children.equals(n.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, children);
    }
}
