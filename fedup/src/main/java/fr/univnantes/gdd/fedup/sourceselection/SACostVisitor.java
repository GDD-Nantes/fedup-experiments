package fr.univnantes.gdd.fedup.sourceselection;

import com.fluidops.fedx.algebra.*;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

public class SACostVisitor extends AbstractQueryModelVisitor<Exception> {

    private int cost = 0;

    public int getCost() {
        return cost;
    }

    @Override
    public void meetOther(QueryModelNode node) throws Exception {
        if (node instanceof StatementTupleExpr) {
            this.meetStatementTupleExpr((StatementTupleExpr) node);
        } else {
            super.meetOther(node);
        }
    }

    protected void meetStatementTupleExpr(StatementTupleExpr node) throws Exception {
        this.cost += node.getStatementSources().size();
    }
}
