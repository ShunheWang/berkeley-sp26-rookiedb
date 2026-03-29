package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextLeftBlock() {
            // 1. 如果left表无任何数据, 说明扫描完毕
            if(!this.leftSourceIterator.hasNext()){
                this.leftBlockIterator = null;
                this.leftRecord = null;
                return;
            }
            // 当前leftSourceIterator继续往后面读 b-2个页数据
            this.leftBlockIterator = QueryOperator.getBlockIterator(
                    this.leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2
            );

            if(this.leftBlockIterator.hasNext()){
                this.leftBlockIterator.markNext();
                this.leftRecord = this.leftBlockIterator.next();
            } else {
                this.leftRecord = null;
            }
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextRightPage() {
            if(!this.rightSourceIterator.hasNext()) return;

            this.rightPageIterator = QueryOperator.getBlockIterator(
                    this.rightSourceIterator, getRightSource().getSchema(), 1
            );

            if(this.rightPageIterator != null) {
                this.rightPageIterator.markNext();
            }
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            while (true) {
                // CASE 1: 当前右页还有数据, 直接匹配
                if(this.rightPageIterator != null && this.rightPageIterator.hasNext()){
                    Record rightRecord = this.rightPageIterator.next();
                    if(this.leftRecord != null && compare(this.leftRecord, rightRecord) == 0){
                        return this.leftRecord.concat(rightRecord);
                    }
                // CASE 2: 右页读完了, 但左块还有数据, 针对的其实是左块中的每一条数据
                } else if(this.leftBlockIterator != null && this.leftBlockIterator.hasNext()){
                    this.leftRecord = this.leftBlockIterator.next();
                    // 因为左块迭代器做了移动，每一个迭代器的行为应该是匹配右侧每一个值，所以这里要重置右迭代器
                    // 假设左块: [1..100], 右页: [A..Z]
                    // 进来CASE 2的条件为: 对于1来说, 已经把A..Z都匹配过了, 如果有匹配中的, CASE 1 已经输出过了,
                    // 现在要把1 推到下一条数据, 也就是2, 此时rightPageIterator 现在是Z
                    // 所以需要reset 重置到A
                    if(this.rightPageIterator != null) {
                        this.rightPageIterator.reset();
                    }
                } else {
                    // CASE 3: 左块与右页数据都读完, 针对的是右页
                    // [A..Z] 读完, 获取下一个右页数据[AA..ZZ]
                    this.fetchNextRightPage();
                    // 如果有[AA..ZZ], 那么对于左块来说要重置回到1, 因为CASE 1和2, 已经把[1..50] 与[A..Z] 匹配完毕,
                    // 如果存在[AA..ZZ] 那么将要匹配的是[1..50] 与[AA..ZZ]
                    if(this.rightPageIterator != null && this.rightPageIterator.hasNext()){
                        // 重置左块到开始位置，重新扫描左块
                        if(this.leftBlockIterator != null) {
                            this.leftBlockIterator.reset();
                            if(this.leftBlockIterator.hasNext()) {
                                this.leftRecord = this.leftBlockIterator.next();
                                // 这里不能 markNext, 也不需要 markNext
                            } else {
                                this.leftRecord = null;
                            }
                        }
                    }
                    // CASE 4: 对应左块 [1..50] 已经把对应的右页[A..Z], [AA..ZZ], [AAA..ZZZ]... 全部匹配过
                    // 那么将左块推到下一个左块数据去[51..100]
                    else {
                        this.fetchNextLeftBlock();
                        if(this.leftBlockIterator == null || !this.leftBlockIterator.hasNext()){
                            return null;
                        }
                        // 这时候需要重置的是 rightSourceIterator 会到1, 而不是rightPageIterator
                        this.rightSourceIterator.reset();
                        this.fetchNextRightPage();
                    }
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
