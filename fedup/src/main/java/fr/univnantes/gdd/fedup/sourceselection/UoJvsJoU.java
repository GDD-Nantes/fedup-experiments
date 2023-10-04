package fr.univnantes.gdd.fedup.sourceselection;

import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.optimizer.Optimizer;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;
import fr.univnantes.gdd.fedup.Spy;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.QueryParserUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UoJvsJoU {

    private final FedXConnection connection;

    public UoJvsJoU(FedXConnection connection) {
        this.connection = connection;
    }

    private List<Map<StatementPattern, List<StatementSource>>> generateJoUFromUoJ(List<Map<StatementPattern, List<StatementSource>>> uoj) {
        Map<StatementPattern, List<StatementSource>> jou = new HashMap<>();
        for (Map<StatementPattern, List<StatementSource>> uojUnit : uoj) {
            for (StatementPattern statementPattern : uojUnit.keySet()) {
                if (!jou.containsKey(statementPattern)) {
                    jou.put(statementPattern, new ArrayList<>());
                }
                for (StatementSource source : uojUnit.get(statementPattern)) {
                    if (!jou.get(statementPattern).contains(source)) {
                        jou.get(statementPattern).add(source);
                    }
                }
            }
        }
        return List.of(jou);
    }

    private int computeSACost(String queryString, List<Map<StatementPattern, List<StatementSource>>> assignment) throws Exception {
        SourceAssignmentsSingleton sourceAssignmentsSingleton = SourceAssignmentsSingleton.getInstance();
        sourceAssignmentsSingleton.setAssignments(assignment);

        ParsedOperation query = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, queryString, null);
        QueryInfo qInfo = new QueryInfo(this.connection, queryString, QueryType.SELECT, this.connection.getSummary());
        TupleExpr tupleExpr = ((ParsedQuery)query).getTupleExpr();

        int cost = 0;
        while (sourceAssignmentsSingleton.hasNextAssignment()) {
            TupleExpr queryPlan = Optimizer.optimize(tupleExpr, new SimpleDataset(), EmptyBindingSet.getInstance(), this.connection.getStrategy(), qInfo);

            SACostVisitor visitor = new SACostVisitor();
            queryPlan.visit(visitor);
            cost += visitor.getCost();
        }
        return cost;
    }

    public List<Map<StatementPattern, List<StatementSource>>> selectBestAssignment(String queryString, List<Map<StatementPattern, List<StatementSource>>> uoj, Spy spy) throws Exception {
        int uojCost = this.computeSACost(queryString, uoj);

        List<Map<StatementPattern, List<StatementSource>>> jou = this.generateJoUFromUoJ(uoj);
        int jouCost = this.computeSACost(queryString, jou);

        if (uojCost <= jouCost) {
            spy.planType = "UoJ";
            return uoj;
        } else {
            spy.planType = "JoU";
            return jou;
        }
    }
}
