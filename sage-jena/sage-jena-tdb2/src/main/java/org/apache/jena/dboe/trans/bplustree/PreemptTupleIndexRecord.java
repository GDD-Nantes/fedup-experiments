package org.apache.jena.dboe.trans.bplustree;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.base.record.RecordMapper;
import org.apache.jena.dboe.index.RangeIndex;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.NodeIdFactory;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;

import java.util.Iterator;

import static org.apache.jena.tdb2.sys.SystemTDB.SizeOfNodeId;

/**
 * Wraps {@link TupleIndexRecord} to return our own iterators instead of default TDB's ones.
 */
public class PreemptTupleIndexRecord {

    RecordFactory recordFactory;
    RangeIndex index;
    TupleMap tupleMap;
    
    public TupleIndexRecord tir;
    BPlusTree bpt;

    private final RecordMapper<Tuple<NodeId>> recordMapper;

    public RecordMapper<Tuple<NodeId>> getRecordMapper() {
        return recordMapper;
    }

    public PreemptTupleIndexRecord(TupleIndexRecord tir) {
        bpt = (BPlusTree) tir.getRangeIndex();
        recordFactory = bpt.getRecordFactory();
        tupleMap = tir.getMapping();

        final int keyLen = recordFactory.keyLength();
        final int numNodeIds = recordFactory.keyLength() / NodeId.SIZE;

        this.index = tir.getRangeIndex(); 
        this.tir = tir;

        recordMapper = (bb, entryIdx, key, recFactory) -> {
            // Version one. (Skipped) : index-order Tuple<NodeId>, then remap.
            // Version two.
            //   Straight to right order.
            // Version three
            //   Straight to right order. Delay creation.

            int bbStart = entryIdx*recFactory.recordLength();
            // Extract the bytes, index order for the key test..
            if ( key != null ) {
                bb.position(bbStart);
                bb.get(key, 0, keyLen);
            }

            // Now directly create NodeIds, no Record.
            NodeId[] nodeIds = new NodeId[numNodeIds];
            for ( int i = 0; i < numNodeIds ; i++ ) {
                int j = i;
                if ( tupleMap != null )
                    j = tupleMap.unmapIdx(i);
                // Get data. It is faster to get from the ByteBuffer than from the key byte[].
                NodeId id = NodeIdFactory.get(bb, bbStart+j*NodeId.SIZE);
                nodeIds[i] = id;
            }
            return TupleFactory.create(nodeIds);
        };
    }

    public PreemptJenaIterator scan(Tuple<NodeId> patternNaturalOrder) {
        Tuple<NodeId> pattern = tupleMap.map(patternNaturalOrder);
        
        // Canonical form.
        int numSlots = 0;
        int leadingIdx = -2;  // Index of last leading pattern NodeId.  Start less than numSlots-1
        boolean leading = true;

        // Records.
        Record minRec = recordFactory.createKeyOnly();
        Record maxRec = recordFactory.createKeyOnly();

        // Set the prefixes.
        for ( int i = 0; i < pattern.len() ; i++ ) {
            NodeId X = pattern.get(i);
            if ( NodeId.isAny(X) ) {
                X = null;
                // No longer setting leading key slots.
                leading = false;
                continue;
            }
            // if ( NodeId.isDoesNotExist(X) )
            //     return new PreemptJenaIterator();

            numSlots++;
            if ( leading ) {
                leadingIdx = i;
                NodeIdFactory.set(X, minRec.getKey(), i*SizeOfNodeId);
                NodeIdFactory.set(X, maxRec.getKey(), i*SizeOfNodeId);
            }
        }

        // Is it a simple existence test?
        if ( numSlots == pattern.len() ) {
             if ( index.contains(minRec) ) {
                 // We slightly lose in efficiency here by searching into the btree instead of
                 // creating a `SingletonIterator` but it enables easy pause/resume.
                 NodeId X = pattern.get(leadingIdx);
                 // Set the max Record to the leading NodeIds, +1.
                 // Example, SP? inclusive to S(P+1)? exclusive where ? is zero.
                 NodeIdFactory.setNext(X, maxRec.getKey(), leadingIdx*SizeOfNodeId);
                 return new PreemptJenaIterator(this, pattern);
             } else {
                 return new PreemptJenaIterator(); // null iterator
             }
         }
        
        PreemptJenaIterator tuples;
        if ( leadingIdx < 0 ) {
            // fullScan always allowed
            // if ( ! fullScanAllowed )
            // return null;
            // Full scan necessary
            tuples = new PreemptJenaIterator(this, null, null);
        } else {
            // Adjust the maxRec.
            NodeId X = pattern.get(leadingIdx);
            // Set the max Record to the leading NodeIds, +1.
            // Example, SP? inclusive to S(P+1)? exclusive where ? is zero.
            NodeIdFactory.setNext(X, maxRec.getKey(), leadingIdx*SizeOfNodeId);

            tuples = new PreemptJenaIterator(this, minRec, maxRec);
        }
        
        if ( leadingIdx < numSlots-1 ) {
            // partial scan always allowed
            // if ( ! partialScanAllowed )
            // return null;
            // Didn't match all defined slots in request.
            // Partial or full scan needed.
            //pattern.unmap(colMap);

            // Method scanMethod = ReflectionUtils._getMethod(TupleIndexRecord.class, "scan");
            // tuples = scan(tuples, patternNaturalOrder);
            // tuples = (Iterator<Tuple<NodeId>>) ReflectionUtils._callMethod(scanMethod, tir.getClass(), tir,
            // tuples, patternNaturalOrder);
            // (TODO) double check this part.
            tuples = new PreemptJenaIterator(this, null, null);
        }
        
        return tuples;
    }

    /* *************** More generic scan **********************/

    public class IteratorBuilder {
        public final PreemptTupleIndexRecord ptir;
        public final Record min;
        public final Record max;
        public final Tuple<NodeId> pattern;

        public IteratorBuilder(PreemptTupleIndexRecord ptir, Record min, Record max, Tuple<NodeId> pattern) {
            this.ptir = ptir;
            this.min = min;
            this.max = max;
            this.pattern = pattern;
        }
    }


    public IteratorBuilder genericScan(Tuple<NodeId> patternNaturalOrder) {
        Tuple<NodeId> pattern = tupleMap.map(patternNaturalOrder);

        // Canonical form.
        int numSlots = 0;
        int leadingIdx = -2;  // Index of last leading pattern NodeId.  Start less than numSlots-1
        boolean leading = true;

        // Records.
        Record minRec = recordFactory.createKeyOnly();
        Record maxRec = recordFactory.createKeyOnly();

        // Set the prefixes.
        for ( int i = 0; i < pattern.len() ; i++ ) {
            NodeId X = pattern.get(i);
            if ( NodeId.isAny(X) ) {
                X = null;
                // No longer setting leading key slots.
                leading = false;
                continue;
            }
            // if ( NodeId.isDoesNotExist(X) )
            //     return new PreemptJenaIterator();

            numSlots++;
            if ( leading ) {
                leadingIdx = i;
                NodeIdFactory.set(X, minRec.getKey(), i*SizeOfNodeId);
                NodeIdFactory.set(X, maxRec.getKey(), i*SizeOfNodeId);
            }
        }

        // Is it a simple existence test?
        if ( numSlots == pattern.len() ) {
            if ( index.contains(minRec) ) {
                // We slightly lose in efficiency here by searching into the btree instead of
                // creating a `SingletonIterator` but it enables easy pause/resume.
                NodeId X = pattern.get(leadingIdx);
                // Set the max Record to the leading NodeIds, +1.
                // Example, SP? inclusive to S(P+1)? exclusive where ? is zero.
                NodeIdFactory.setNext(X, maxRec.getKey(), leadingIdx*SizeOfNodeId);
                return new IteratorBuilder(this, minRec, maxRec, pattern);
            } else {
                return new IteratorBuilder(null, null, null, null); // null iterator
            }
        }

        IteratorBuilder tuples;
        if ( leadingIdx < 0 ) {
            // fullScan always allowed
            // if ( ! fullScanAllowed )
            // return null;
            // Full scan necessary
            tuples = new IteratorBuilder(this, null, null, null);
        } else {
            // Adjust the maxRec.
            NodeId X = pattern.get(leadingIdx);
            // Set the max Record to the leading NodeIds, +1.
            // Example, SP? inclusive to S(P+1)? exclusive where ? is zero.
            NodeIdFactory.setNext(X, maxRec.getKey(), leadingIdx*SizeOfNodeId);

            tuples = new IteratorBuilder(this, minRec, maxRec, null);
        }

        if ( leadingIdx < numSlots-1 ) {
            // partial scan always allowed
            // if ( ! partialScanAllowed )
            // return null;
            // Didn't match all defined slots in request.
            // Partial or full scan needed.
            //pattern.unmap(colMap);

            // Method scanMethod = ReflectionUtils._getMethod(TupleIndexRecord.class, "scan");
            // tuples = scan(tuples, patternNaturalOrder);
            // tuples = (Iterator<Tuple<NodeId>>) ReflectionUtils._callMethod(scanMethod, tir.getClass(), tir,
            // tuples, patternNaturalOrder);
            // (TODO) double check this part.
            tuples = new IteratorBuilder(this, null, null, null);
        }

        return tuples;
    }

}
