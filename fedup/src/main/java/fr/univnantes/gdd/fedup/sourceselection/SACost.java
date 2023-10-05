package fr.univnantes.gdd.fedup.sourceselection;

import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.algebra.*;
import com.fluidops.fedx.optimizer.Optimizer;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.In;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.eclipse.rdf4j.query.parser.QueryParserUtil.*;

public class SACost extends AbstractQueryModelVisitor<Exception> {

    private int cost = 0;

    public static List<Integer> compute(FedXConnection connection, String queryString, List<Map<StatementPattern, List<StatementSource>>> assignments) throws Exception {
        List<Integer> costs = new ArrayList<>();

        SourceAssignmentsSingleton singleton = SourceAssignmentsSingleton.getInstance();
        singleton.setAssignments(assignments);

        ParsedOperation query = parseOperation(QueryLanguage.SPARQL, queryString, null);
        QueryInfo qInfo = new QueryInfo(connection, queryString, QueryType.SELECT, connection.getSummary());
        TupleExpr tupleExpr = ((ParsedQuery)query).getTupleExpr();

        while (singleton.hasNextAssignment()) {
            TupleExpr queryPlan = Optimizer.optimize(tupleExpr, new SimpleDataset(), EmptyBindingSet.getInstance(), connection.getStrategy(), qInfo);

            SACost visitor = new SACost();
            queryPlan.visit(visitor);
            costs.add(visitor.cost);
        }

        return costs;
    }

    public static int compute(FedXConnection connection, String queryString, Map<StatementPattern, List<StatementSource>> assignment) throws Exception {
        return compute(connection, queryString, List.of(assignment)).get(0);
    }

    @Override
    public void meetOther(QueryModelNode node) throws Exception {
        if (node instanceof StatementTupleExpr) {
            this.meetStatementTupleExpr((StatementTupleExpr) node);
        } else {
            super.meetOther(node);
        }
    }

    protected void meetStatementTupleExpr(StatementTupleExpr node) {
        this.cost += node.getStatementSources().size();
    }
}
