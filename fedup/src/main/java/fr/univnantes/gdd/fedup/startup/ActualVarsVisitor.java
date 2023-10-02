package fr.univnantes.gdd.fedup.startup;

import fr.gdd.raw.io.OpVisitorUnimplemented;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;

import java.util.Set;

/**
 * Explore to get the actual visible variables of the query.
 */
public class ActualVarsVisitor extends OpVisitorByType {

    public Set<Var> vars;

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
            case OpProject o -> {
                vars = OpVars.visibleVars(o.getSubOp());
            }
            default -> op.getSubOp().visit(this);

        }

    }

    @Override
    protected void visit0(Op0 op) {}

    @Override
    protected void visitFilter(OpFilter op) {}

    @Override
    protected void visitLeftJoin(OpLeftJoin op) {}
}
