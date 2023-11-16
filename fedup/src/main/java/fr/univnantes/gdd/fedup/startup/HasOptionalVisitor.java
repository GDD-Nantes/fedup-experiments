package fr.univnantes.gdd.fedup.startup;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.op.*;

public class HasOptionalVisitor extends OpVisitorByType {

    public boolean result = false;

    @Override
    protected void visitN(OpN op) {
        for (Op sub : op.getElements()) {
            sub.visit(this);
        }
    }

    @Override
    protected void visit2(Op2 op) {
        switch (op) {
            case OpConditional o -> result = true;
            default -> {
                op.getLeft().visit(this);
                op.getRight().visit(this);
            }
        }
    }

    @Override
    protected void visit1(Op1 op) {
        op.getSubOp().visit(this);
    }

    @Override
    protected void visit0(Op0 op) {
    }

    @Override
    protected void visitFilter(OpFilter op) {
    }

    @Override
    protected void visitLeftJoin(OpLeftJoin op) {
        result = true;
    }
}
