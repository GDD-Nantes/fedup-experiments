package fr.univnantes.gdd.fedup.summary;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;

public class RemoveFilterTransformerLol extends TransformCopy {

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        return subOp;
    }

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        return new OpConditional(left, right);
    }
}
