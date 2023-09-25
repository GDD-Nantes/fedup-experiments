package fr.univnantes.gdd.fedup.summary;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpFilter;

public class RemoveFilterTransformerLol extends TransformCopy {

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        return subOp;
    }
}
