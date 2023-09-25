package org.apache.jena.dboe.trans.bplustree;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.base.buffer.RecordBuffer;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.trans.bplustree.AccessPath.AccessStep;
import org.apache.jena.tdb2.store.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;

/**
 * An iterator that allows measuring the estimated progress of execution, i.e.,
 * the number of explored elements over the estimated number to explore.
 */
public class ProgressJenaIterator {

    private static Logger log = LoggerFactory.getLogger(ProgressJenaIterator.class);

    /**
     * Number of walks to approximate how filled bptree's records are.
     */
    public static int NB_WALKS = 2; // (TODO) could be self-adaptive, and depend on the number of possible nodes between bounds

    long offset = 0; // the number of elements explored
    Double cardinality = null; // lazy loaded cardinality

    private final Record minRecord;
    private final Record maxRecord;

    private final BPTreeNode root;
    private final PreemptTupleIndexRecord ptir;

    public ProgressJenaIterator(PreemptTupleIndexRecord ptir, Record minRec, Record maxRec) {
        this.root = ptir.bpt.getNodeManager().getRead(ptir.bpt.getRootId());
        this.minRecord = Objects.isNull(minRec) ? root.minRecord() : minRec;
        this.maxRecord = Objects.isNull(maxRec) ? root.maxRecord() : maxRec;
        this.ptir = ptir;
    }

    /**
     * Empty iterator. Cardinality is zero.
     */
    public ProgressJenaIterator() {
        this.cardinality = 0.;
        this.maxRecord = null;
        this.minRecord = null;
        this.root = null;
        this.ptir = null;
    }

    /**
     * Singleton iterator. Cardinality is one.
     */
    public ProgressJenaIterator(PreemptTupleIndexRecord ptir, Tuple<NodeId> pattern) {
        this.cardinality = 1.;
        this.root = ptir.bpt.getNodeManager().getRead(ptir.bpt.getRootId());
        this.maxRecord = null;
        this.minRecord = null;
        this.ptir = null;
    }

    public void next() {
        this.offset += 1;
    }

    public long getOffset() {
        return offset;
    }

    public Serializable current() {
        return offset;
    }

    public Serializable previous() {
        return offset - 1;
    }

    public void skip(Serializable to) {
        this.offset = (Long) to;
    }

    public double getProgress() {
        if (Objects.isNull(cardinality)) {
            this.cardinality();
        }

        if (cardinality == 0.) {
            return 1.0;
        } // already finished

        return ((double) this.offset) / cardinality;
    }

    /**
     * Counts the number of elements by iterating over them. Warning: this is costly, but it provides exact
     * cardinality. Mostly useful for debugging purposes.
     * @return The exact number of elements in the iterator.
     */
    public long count() {
        if (Objects.isNull(ptir)) {
            return 0L;
        }
        Iterator<Tuple<NodeId>> wrapped = ptir.bpt.iterator(minRecord, maxRecord, ptir.getRecordMapper());
        long nbElements = 0;
        while (wrapped.hasNext()) {
            wrapped.next();
            nbElements += 1;
        }
        return nbElements;
    }

    /**
     * Convenience function that checks the equality of two access paths.
     **/
    private static boolean equalsStep(AccessPath.AccessStep o1, AccessPath.AccessStep o2) {
        return o1.node.getId() == o2.node.getId() &&
                o1.idx == o2.idx &&
                o1.page.getId() == o2.page.getId();
    }


    private boolean isParent(AccessPath randomWalk, AccessPath base, AccessPath other) {
        for (int i = 0; i < base.getPath().size(); ++i) {
            if (base.getPath().get(i).node.id == other.getPath().get(i).node.id) {
                continue;
            }
            if (randomWalk.getPath().get(i).node.id == base.getPath().get(i).node.id) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieves a random record in between boundaries along with the probability of getting
     * retrieved, following the wander join formula.
     * @param minPath The smaller boundary.
     * @param maxPath The higher boundary.
     * @return A pair comprising the random record and its probability of getting drawn.
     */
    private ImmutablePair<Record, Double> randomWalkWJ(AccessPath minPath, AccessPath maxPath) {
        assert minPath.getPath().size() == maxPath.getPath().size();
        
        int idxMin = minPath.getPath().get(0).idx;
        int idxMax = maxPath.getPath().get(0).idx;

        int idxRnd = (int) (idxMin + Math.random() * (idxMax - idxMin + 1));

        BPTreeNode node = minPath.getPath().get(0).node;
        AccessPath.AccessStep lastStep = new AccessStep(node, idxRnd, node.get(idxRnd));
        double proba = 1.0 / (idxMax - idxMin + 1.0);

        while (!lastStep.node.isLeaf()) {
            node = (BPTreeNode) lastStep.page;

            idxMin = node.findSlot(minRecord);
            idxMax = node.findSlot(maxRecord);

            idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;
            idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

            idxRnd = (int) (idxMin + Math.random() * (idxMax - idxMin + 1));
            
            lastStep = new AccessStep(node, idxRnd, node.get(idxRnd));
            proba *= 1.0 / (idxMax - idxMin + 1.0);
        }

        RecordBuffer recordBuffer = ((BPTreeRecords) lastStep.page).getRecordBuffer();

        idxMin = recordBuffer.find(minRecord);
        idxMax = recordBuffer.find(maxRecord);
        
        idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;
        idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;

        if (idxMin == idxMax) {
            return new ImmutablePair<Record,Double>(null, 0.0);
        }

        idxRnd = (int) (idxMin + Math.random() * (idxMax - idxMin)); // no need for -1 in a `RecordBuffer`
        
        proba *= 1.0 / (idxMax - idxMin);

        return new ImmutablePair<Record,Double>(recordBuffer.get(idxRnd), proba);
    }

    /**
     * @return A random record between the set boundaries of the object.
     */
    public Record random() {
        AccessPath minPath = new AccessPath(null);
        AccessPath maxPath = new AccessPath(null);

        root.internalSearch(minPath, minRecord);
        root.internalSearch(maxPath, maxRecord);
        ImmutablePair<Record, Double> recordAndProba = randomWalkWJ(minPath, maxPath);

        return recordAndProba.getLeft();
    }

    /**
     * Estimates the cardinality of a triple/quad pattern knowing that
     * the underlying data structure is a balanced tree.
     * When the number of results is small, more precision is needed.
     * Fortunately, this often means that results are spread among one
     * or two pages, which allows us to precisely count using binary search.
     *
     * @return An estimated cardinality.
     */
    public double cardinality(Integer... sample) {
        if (Objects.isNull(minRecord) && Objects.isNull(maxRecord) && Objects.isNull(root)) {
            return 0;
        }

        // number of random walks to estimate cardinalities in between boundaries.
        int nbWalks = Objects.isNull(sample) || sample.length == 0 ? NB_WALKS : sample[0];

        if (nbWalks == Integer.MAX_VALUE) {
            // MAX_VALUE goes for counting since it's the most costly, at least we want exact cardinality
            return count();
        }

        if (Objects.nonNull(this.cardinality)) {
            return cardinality; // already processed, lazy return.
        }

        AccessPath minPath = new AccessPath(null);
        AccessPath maxPath = new AccessPath(null);

        root.internalSearch(minPath, minRecord);
        root.internalSearch(maxPath, maxRecord);

        AccessPath.AccessStep minStep = minPath.getPath().get(minPath.getPath().size() - 1);
        AccessPath.AccessStep maxStep = maxPath.getPath().get(maxPath.getPath().size() - 1);

        double cardinality = 0;

        // exact count for the leftmost and rightmost page
        RecordBuffer minRecordBuffer = ((BPTreeRecords) minStep.page).getRecordBuffer();
        RecordBuffer maxRecordBuffer = ((BPTreeRecords) maxStep.page).getRecordBuffer();
        
        int idxMin = minRecordBuffer.find(minRecord);
        int idxMax =  maxRecordBuffer.find(maxRecord);

        idxMax = idxMax < 0 ? -idxMax - 1 : idxMax;
        idxMin = idxMin < 0 ? -idxMin - 1 : idxMin;

        if (equalsStep(minStep, maxStep)) {
            cardinality += idxMax - idxMin;
        } else {
            cardinality += minRecordBuffer.size() - idxMin;
            cardinality += idxMax;
        }

        // random walks to estimate the number of records between the leftmost and rightmost page
        if (minStep.node.id != maxStep.node.id || maxStep.idx - minStep.idx >= 2) {
            double sum = 0.0;
            int count = 0;

            for (int i = 0; i < nbWalks; i++) {
                ImmutablePair<Record, Double> pair = this.randomWalkWJ(minPath, maxPath);
                // records in the leftmost and rightmost page are ignored
                if (pair.getLeft() != null && minRecordBuffer.find(pair.getLeft()) < 0 && maxRecordBuffer.find(pair.getLeft()) < 0) {
                    sum += 1 / pair.getRight();
                }
                count += 1;
            }
            cardinality += sum / (double) count;
        }

        this.cardinality = cardinality; // for laziness

        return this.cardinality;
    }

}