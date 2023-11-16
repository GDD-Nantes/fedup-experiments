package fr.univnantes.gdd.fedup.startup;

import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.optimizer.Optimizer;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;
import fr.univnantes.gdd.fedup.sourceselection.SourceAssignmentsSingleton;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;

import java.util.List;
import java.util.Map;

public class PrintQueryPlans {

    private final FedXConnection connection;

    public PrintQueryPlans(FedXConnection connection) {
        this.connection = connection;
    }

    public void print(String queryString, List<Map<StatementPattern, List<StatementSource>>> assignments) {
        SourceAssignmentsSingleton sourceAssignmentsSingleton = SourceAssignmentsSingleton.getInstance();
        sourceAssignmentsSingleton.setAssignments(assignments);

        ParsedOperation query = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, queryString, null);
        QueryInfo qInfo = new QueryInfo(this.connection, queryString, QueryType.SELECT, this.connection.getSummary());
        TupleExpr tupleExpr = ((ParsedQuery)query).getTupleExpr();

        while (sourceAssignmentsSingleton.hasNextAssignment()) {
            TupleExpr queryPlan = Optimizer.optimize(tupleExpr, new SimpleDataset(), EmptyBindingSet.getInstance(), this.connection.getStrategy(), qInfo);
            System.out.println("-".repeat(20));
            System.out.println(queryPlan);
        }
        System.out.println("-".repeat(20));
    }
}
