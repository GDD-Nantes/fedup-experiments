package fr.gdd.sage.arq;

import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleMap;
import org.apache.jena.dboe.base.record.Record;
import org.apache.jena.dboe.base.record.RecordFactory;
import org.apache.jena.dboe.base.record.RecordMapper;
import org.apache.jena.dboe.trans.bplustree.BPlusTree;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;

import java.util.Iterator;

/**
 * An iterator factory that eases the switch between one another iterator.
 * Amongst them, the scan iterator getter proves particularly handy since
 * the constructor of such an iterator appears late after the `execute`
 * of the corresponding operator.
 *
 * So the code remains the same, only the factory is different.
 */
public interface ScanIteratorFactory {

    /**
     * @param id The identifier of the operator inherited by the iterator.
     * @return An empty scan iterator
     */
    Iterator<Tuple<NodeId>> getScan(Integer id);

    /**
     * @param pattern The pattern to look for.
     * @param id The identifier of the operator inherited by the iterator.
     * @return A scan iterator based on the pattern of {@link NodeId}.
     */
    Iterator<Quad> getScan(Tuple<NodeId> pattern, Integer id);

    /**
     * @param nodeTupleTable The node tuple table from where the pattern node ids come from
     * @param pattern The pattern to look for.
     * @param id The identifier of the operator inherited by the iterator.
     * @return A scan iterator based on the pattern of {@link NodeId}.
     */
    Iterator<Tuple<NodeId>> getScan(NodeTupleTable nodeTupleTable, Tuple<NodeId> pattern, Integer id);
}
