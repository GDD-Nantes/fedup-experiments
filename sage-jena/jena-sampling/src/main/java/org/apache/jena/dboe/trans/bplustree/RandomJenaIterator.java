package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.interfaces.RandomIterator;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.NodeId;

import java.util.*;

/**
 * This {@link RandomJenaIterator} enables random exploration of patterns.
 * This heavily depends on the {@link BPlusTree}
 * data structure, since it relies on {@link AccessPath} to
 * find out the boundary of the scan and draw a random element from
 * it.
 *
 * This is inspired from {@link BPTreeRangeIterator}.
 **/
public class RandomJenaIterator implements Iterator<Tuple<NodeId>>, RandomIterator {

    final private Record minRecord;
    final private Record maxRecord;

    private Tuple<NodeId> current;
    private TupleMap tupleMap;
    private BPTreeNode root;

    boolean first = true;

    public RandomJenaIterator(PreemptTupleIndexRecord ptir, Record minRec, Record maxRec) {
        this.root = ptir.bpt.getNodeManager().getRead(ptir.bpt.getRootId());
        this.tupleMap = ptir.tupleMap;
        this.minRecord = minRec;
        this.maxRecord = maxRec;
    }

    @Override
    public boolean hasNext() {
        if (first && random()) {
            first = false;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Tuple<NodeId> next() {
        return current;
    }

    /**
     * Cardinality estimation exploiting the fact that the underlying
     * data structure is a balanced tree. When the number of results
     * is small, more precision is needed. Fortunately, this often
     * means that results are spread among one or two pages which
     * allows us to precisely count using binary search.
     *
     * (TODO) Take into account possible deletions.
     */
    @Override
    public long cardinality() {
        if (Objects.isNull(minRecord) && Objects.isNull(maxRecord)) {
            return 0; // empty iterator
        }
        
        AccessPath minPath = new AccessPath(null);
        root.internalSearch(minPath, minRecord);
        AccessPath maxPath = new AccessPath(null);
        root.internalSearch(maxPath, maxRecord);

        AccessPath randomPath = new AccessPath(null); // to build
        // #1 process common branch
        var minSteps = minPath.getPath();
        var maxSteps = maxPath.getPath();

        int i = 0;
        AccessPath.AccessStep minStep = minSteps.get(i);
        AccessPath.AccessStep maxStep = maxSteps.get(i);

        boolean done = false;
        while (i < minSteps.size() && i < maxSteps.size() && equalsStep(minStep, maxStep)) {
            randomPath.add(minStep.node, minStep.idx, minStep.page);
            ++i;
            if (i < minSteps.size() && i < maxSteps.size()) { // ugly
                minStep = minSteps.get(i);
                maxStep = maxSteps.get(i);
            } else {
                done = true;
            }
        }

        // done with common branch
        // #2 random between the remainder of the branches
        BPTreeNode lastMinNode = minStep.node;
        BPTreeNode lastMaxNode = maxStep.node;

        // #A if last was same node but different pages
        long sum = 0;
        BPTreePage minPage = minStep.page;
        BPTreePage maxPage = maxStep.page;
        if (!done && lastMinNode.getId() == lastMaxNode.getId()) {
            int idxMin  = minStep.idx;
            int idxMax  = maxStep.idx;
            sum = idxMax - idxMin;

            minPage = minStep.page;
            maxPage = maxStep.page;

            // #B random among the whole range of the underlying branch
            while (!(minPage instanceof BPTreeRecords)) {
                i = i + 1;
                minStep = minSteps.get(i);
                maxStep = maxSteps.get(i);

                sum = (sum - 2)*root.getMaxSize() + minStep.node.getMaxSize() - minStep.idx + maxStep.idx;
                minPage = minStep.page;
                maxPage = maxStep.page;
            }
        }


        BPTreeRecords minBPTreeRecord = (BPTreeRecords) minPage;
        BPTreeRecords maxBPTreeRecord = (BPTreeRecords) maxPage;

        RecordBuffer minRecordBuffer = minBPTreeRecord.getRecordBuffer();
        RecordBuffer maxRecordBuffer = maxBPTreeRecord.getRecordBuffer();

        int min = minRecordBuffer.find(minRecord);
        int max = maxRecordBuffer.find(maxRecord);

        min = min < 0 ? -min-1 : min;
        max = max < 0 ? -max-1 : max;

        if (minPage.getId() == maxPage.getId()) {
            sum = max - min;
        } else {
            sum = (sum - 2)*root.getMaxSize() + minStep.page.getCount() - min + max;
        }

        return sum;
    }

    /**
     * `random()` modifies the behavior of the iterator so that each
     * `next()` provides a new random binding within the
     * interval. Beware that it does not terminate nor ensure
     * distinct bindings.
     *
     * As for `cardinality()`, it uses the underlying balanced tree to
     * efficiently reach a Record between two access paths.
     *
     * @return True if the random can return a solution, false otherwise.
     */
    @Override
    public boolean random() {
        AccessPath minPath = new AccessPath(null);
        root.internalSearch(minPath, minRecord);
        AccessPath maxPath = new AccessPath(null);
        root.internalSearch(maxPath, maxRecord);

        AccessPath randomPath = new AccessPath(null); // to build
        // #1 process common branch
        var minSteps = minPath.getPath();
        var maxSteps = maxPath.getPath();
        
        int i = 0;
        AccessPath.AccessStep minStep = minSteps.get(i);
        AccessPath.AccessStep maxStep = maxSteps.get(i);

        boolean done = false;
        while (i < minSteps.size() && i < maxSteps.size() && equalsStep(minStep, maxStep)) {
            randomPath.add(minStep.node, minStep.idx, minStep.page);
            ++i;
            if (i< minSteps.size() && i < maxSteps.size()) { // ugly
                minStep = minSteps.get(i);
                maxStep = maxSteps.get(i);
            } else {
                done = true;
            }
        }
        
        // done with common branch
        // #2 random between the remainder of the branches
        BPTreeNode lastMinNode = minStep.node; 
        BPTreeNode lastMaxNode = maxStep.node;

        // #A if last was same node but different pages
        if (!done && lastMinNode.getId() == lastMaxNode.getId()) {
            int idxMin  = minStep.idx;
            int idxMax  = maxStep.idx; 
            int found_idx = (int) ((double)idxMin + Math.random() * ((double)(idxMax - idxMin + 1)));
            
            BPTreePage chosenPage = lastMinNode.get(found_idx);
            randomPath.add(lastMinNode, found_idx, chosenPage);
            
            // #B random among the whole range of the underlying branch
            while (!(chosenPage instanceof BPTreeRecords)) {
                BPTreeNode chosenNode = (BPTreeNode) chosenPage;
                int min = chosenNode.findSlot(minRecord);
                int max = chosenNode.findSlot(maxRecord);

                min = min < 0 ? -min-1 : min;
                max = max < 0 ? -max-1 : max;

                found_idx = ((int) ((double) min + Math.random()*((double) max - min + 1)));
                chosenPage = ((BPTreeNode) chosenPage).get(found_idx);
                randomPath.add(chosenNode, found_idx, chosenPage);
            }
        }

        // #3 Randomize within the found page
        List<AccessPath.AccessStep> randomSteps = randomPath.getPath();
        AccessPath.AccessStep lastStep = randomSteps.get(randomSteps.size() - 1);
        BPTreePage page = lastStep.page;
        BPTreeRecords record = (BPTreeRecords) page;        
        RecordBuffer recordBuffer = record.getRecordBuffer();
   
        int min = recordBuffer.find(minRecord);
        int max = recordBuffer.find(maxRecord);
        
        min = min < 0 ? -min-1 : min;
        max = max < 0 ? -max-1 : max;

        if (min == max) {
            // No result for the random step, case closed.
            return false;
        }

        int randomInRecords = ((int) ((double) min + Math.random()*((double) max - min))); // no need for -1 in record
        Record currentRecord = recordBuffer._get(randomInRecords);

        current = TupleLib.tuple(currentRecord, tupleMap);
        return true;
    }

    /**
     * Convenience function that checks the equality of two access paths.
     **/
    private static boolean equalsStep(AccessPath.AccessStep o1, AccessPath.AccessStep o2) {
        return o1.node.getId() == o2.node.getId() &&
            o1.idx == o2.idx &&
            o1.page.getId() == o2.page.getId();
    }

}
