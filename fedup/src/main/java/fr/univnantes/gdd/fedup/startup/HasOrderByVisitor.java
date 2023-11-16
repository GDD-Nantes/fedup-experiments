package fr.univnantes.gdd.fedup.startup;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.op.*;

public class HasOrderByVisitor extends OpVisitorByType {

    public OpOrder result = null;

    @Override
    protected void visitN(OpN op) {
        for (Op sub : op.getElements()) {
            sub.visit(this);
        }
    }

    @Override
    protected void visit2(Op2 op) {
        op.getLeft().visit(this);
        op.getRight().visit(this);
    }

    @Override
    protected void visit1(Op1 op) {
        switch (op) {
            case OpOrder o -> result = o;
            default -> op.getSubOp().visit(this);
        }
    }

    @Override
    protected void visit0(Op0 op) {
    }

    @Override
    protected void visitFilter(OpFilter op) {
    }

    @Override
    protected void visitLeftJoin(OpLeftJoin op) {
        op.getLeft().visit(this);
        op.getRight().visit(this);
    }
}
