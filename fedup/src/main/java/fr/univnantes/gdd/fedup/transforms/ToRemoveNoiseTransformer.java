package fr.univnantes.gdd.fedup.transforms;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpSlice;

/**
 * Rempve operators that are noise to source selection.
 */
public class ToRemoveNoiseTransformer extends TransformCopy {

    @Override
    public Op transform(OpDistinct opDistinct, Op subOp) {
        return subOp; // remove DISTINCT
    }

    @Override
    public Op transform(OpSlice opSlice, Op subOp) {
        return subOp; // remove LIMIT
    }

    @Override
    public Op transform(OpProject opProject, Op subOp) {
        return subOp;  // no opProject <=> SELECT *
    }

    @Override
    public Op transform(OpOrder opOrder, Op subOp) {
        return subOp; // no ORDER
    }
}
