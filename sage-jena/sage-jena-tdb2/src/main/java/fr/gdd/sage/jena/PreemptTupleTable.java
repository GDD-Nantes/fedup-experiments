package fr.gdd.sage.jena;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.trans.bplustree.PreemptJenaIterator;
import org.apache.jena.dboe.trans.bplustree.PreemptTupleIndexRecord;
import org.apache.jena.tdb2.TDBException;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.tupletable.TupleIndex;
import org.apache.jena.tdb2.store.tupletable.TupleIndexRecord;
import org.apache.jena.tdb2.store.tupletable.TupleTable;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class PreemptTupleTable extends TupleTable {

    private final PreemptTupleIndexRecord scanAllIndex;
    private final List<PreemptTupleIndexRecord> preemptIndexes = new ArrayList<>(); // wrapped indexes

    // by default, we get:
    // SPO POS OSP
    // GSPO GPOS GOSP
    // POSG OSPG SPOG
    public PreemptTupleTable(TupleTable parent) {
        super(parent.getTupleLen(), parent.getIndexes());

        TupleIndex sai = chooseScanAllIndex(parent.getTupleLen(), parent.getIndexes());
        TupleIndexRecord tir_sai = (TupleIndexRecord) sai;
        scanAllIndex = new PreemptTupleIndexRecord(tir_sai);
        
        for (TupleIndex idx: super.getIndexes()) {
            TupleIndexRecord tir = (TupleIndexRecord) idx;
            PreemptTupleIndexRecord ptir = new PreemptTupleIndexRecord(tir);
            preemptIndexes.add(ptir);
        }
    }

    /** Choose an index to scan in case we are asked for everything
     * This needs to be ???G for the distinctAdjacent filter in union query to work.
     * 
     * Shameless copy from:
     * <https://github.com/apache/jena/blob/54f0f944f97e32add9a48ce6a907f9cf49a10102/jena-tdb2/src/main/java/org/apache/jena/tdb2/store/tupletable/TupleTable.java#L61>
     */
    private static TupleIndex chooseScanAllIndex(int tupleLen, TupleIndex[] indexes) {
        if ( tupleLen != 4 )
            return indexes[0];

        for ( TupleIndex index : indexes ) {
            // First look for SPOG
            if ( index.getName().equals("SPOG") )
                return index;
        }

        for ( TupleIndex index : indexes ) {
            // Then look for any ???G
            if ( index.getName().endsWith("G") )
                return index;
        }

        // Log.warn(SystemTDB.errlog, "Did not find a ???G index for full scans");
        return indexes[0];
    }
    

    // <https://github.com/apache/jena/blob/c4e999d633b2532b504b35937db4bec9a7c2e539/jena-tdb2/src/main/java/org/apache/jena/tdb2/store/tupletable/TupleTable.java#L133>
    public PreemptTupleIndexRecord findIndex(Tuple<NodeId> pattern) {
        if ( super.getTupleLen() != pattern.len() )
            throw new TDBException(format("Mismatch: finding tuple of length %d in a table of tuples of length %d", pattern.len(), super.getTupleLen()));
        
        int numSlots = 0;
        // Canonical form.
        for ( int i = 0; i < super.getTupleLen() ; i++ ) {
            NodeId x = pattern.get(i);
            if ( ! NodeId.isAny(x) )
                numSlots++;
            if ( NodeId.isDoesNotExist(x)) {
                return null;
            }
        }
        
        if (numSlots == 0) {
            return scanAllIndex;
        }
        
        int indexNumSlots = 0;
        PreemptTupleIndexRecord index = null;
        for ( PreemptTupleIndexRecord ptir : preemptIndexes) {
            TupleIndex idx = ptir.tir;
            if ( idx != null ) {
                int w = idx.weight( pattern );
                if ( w > indexNumSlots ) {
                    indexNumSlots = w;
                    index = ptir;
                }
            }
        }

        if ( index == null )
            // No index at all.  Scan.
            index = preemptIndexes.get(0);
        
        return index;
    }

    public PreemptJenaIterator preemptFind(Tuple<NodeId> pattern) {
        PreemptTupleIndexRecord ptir = this.findIndex(pattern);
        // (TODO) if a term does not exist, ptir is null and should be treated
        return ptir.scan(pattern);
    }
    
}
