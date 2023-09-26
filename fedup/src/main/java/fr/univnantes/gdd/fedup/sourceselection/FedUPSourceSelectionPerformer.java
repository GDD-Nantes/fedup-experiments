package fr.univnantes.gdd.fedup.sourceselection;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.Util;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.structures.Endpoint;
import fr.gdd.raw.QueryEngineRAW;
import fr.gdd.raw.RAWConstants;
import fr.gdd.raw.io.RAWInput;
import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.ToSourceSelectionQueryTransform;
import fr.univnantes.gdd.fedup.Utils;
import fr.univnantes.gdd.fedup.summary.Summarizer;
import fr.univnantes.gdd.fedup.transforms.Graph2TripleVisitor;
import fr.univnantes.gdd.fedup.transforms.ToSourceSelectionTransforms;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.queryrender.sparql.SPARQLQueryRenderer;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import java.util.*;
import java.util.stream.Collectors;

public class FedUPSourceSelectionPerformer extends SourceSelectionPerformer {

    private static Logger logger = LogManager.getLogger(FedUPSourceSelectionPerformer.class);

    Set<String> endpoints;
    Dataset ds4Asks;

    public FedUPSourceSelectionPerformer(SailRepositoryConnection connection) throws Exception {
        super(connection);

        // #1 transform the query to get fake ASKs
        endpoints = this.connection.getEndpoints().stream().map(e ->
                e.getEndpoint().substring(e.getEndpoint().indexOf("default-graph-uri=") + 18,
                        e.getEndpoint().length())).collect(Collectors.toSet()); // get graph names from endpoints
        ds4Asks = TDB2Factory.connectDataset(this.connection.getFederation().getConfig().getProperty("fedup.summary"));
    }

    @Override
    public List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(
        String queryString, List<Map<String, String>> optimalAssignments, Spy spy
    ) throws Exception {
        Config config = this.connection.getFederation().getConfig();

        long timeout = Long.parseLong(config.getProperty("fedup.budget", "0"));

        Dataset dataset = TDB2Factory.connectDataset(config.getProperty("fedup.summary"));
        dataset.begin(TxnType.READ);

        QueryEngineFactory factory;
        if (false) { // TODO never random walking
        // if (Boolean.parseBoolean(config.getProperty("fedup.random", "false"))) {
            QueryEngineRAW.register();
            QueryEngineTDB.unregister();
            factory = QueryEngineRAW.factory;
        } else {
            QueryEngineRAW.unregister();
            QueryEngineTDB.register();
            factory = QueryEngineTDB.getFactory();
        }

        var sourceSelectionQuery = this.createSourceSelectionQuery(queryString);


        logger.debug("Executing query...");
        long startTime = System.currentTimeMillis();

        Context context = dataset.getContext().copy()
                .set(RAWConstants.timeout, timeout == 0 ? Long.MAX_VALUE : timeout)
                .set(RAWConstants.input, new RAWInput());

        // Query query = QueryFactory.create(sourceSelectionQuery.getLeft());
        Plan plan = factory.create(sourceSelectionQuery.getLeft(), dataset.asDatasetGraph(), BindingRoot.create(), context);
        QueryIterator iterator = plan.iterator();

        List<Map<String, String>> assignments = new ArrayList<>();
        Set<Integer> seen = new TreeSet<Integer>();

        logger.debug("Getting results...");
        while (iterator.hasNext()) {
            Binding binding = iterator.next();
            int hashcode = binding.toString().hashCode();
            // logger.debug("Binding #" +  Integer.toString(hashcode));
            if (!seen.contains(hashcode)) {
                seen.add(hashcode);
                assignments.add(this.bindingToMap(binding));
                if (optimalAssignments.size() > 0 && this.countMissingAssignments(assignments, optimalAssignments) == 0) {
                    break;
                }
            }
        }
        long endTime = System.currentTimeMillis();
        logger.debug("Query execution terminated...");

        spy.sourceSelectionTime = (endTime - startTime);

        assignments = this.removeInclusions(assignments);

        spy.assignments = assignments;
        spy.numAssignments = assignments.size();
        spy.numValidAssignments = optimalAssignments.size();
        spy.numFoundAssignments = optimalAssignments.size() - this.countMissingAssignments(assignments, optimalAssignments);

        List<Map<StatementPattern, List<StatementSource>>> fedXAssignments = new ArrayList<>();

        Graph2TripleVisitor g2tp = new Graph2TripleVisitor();
        sourceSelectionQuery.getLeft().visit(g2tp);

        Map<Var, StatementPattern> var2bgp = g2tp.getVar2Triple().entrySet().stream().map(e -> {
            OpTriple opTriple = new OpTriple(e.getValue());
            Query query = OpAsQuery.asQuery(opTriple);
            String tripleAsString = query.toString();
            ParsedQuery parseQuery = new SPARQLParser().parseQuery(tripleAsString, "http://donotcare.com/wathever");
            StatementPattern bgp = null;
            try {
                List<List<StatementPattern>> bgps = Utils.getBasicGraphPatterns(parseQuery);
                bgp = bgps.get(0).get(0);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return Map.entry(e.getKey(), bgp);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));


        for (Map<String, String> assignment: assignments) {
            Map<StatementPattern, List<StatementSource>> fedXAssignment = new HashMap<>();
            for (int i = 1; i <= var2bgp.size(); i++) {
                String alias = "g"+i; // TODO change this
                if (assignment.containsKey(alias)) {
                    Endpoint endpoint = Utils.getEndpointByURL(this.connection.getEndpoints(), assignment.get("g" + i));
                    StatementSource source = new StatementSource(endpoint.getId(), StatementSourceType.REMOTE);
                    StatementPattern pattern = var2bgp.get(Var.alloc(alias));
                    fedXAssignment.put(pattern, List.of(source));
                    spy.tpAliases.put(alias, pattern.toString());
                }
            }
            fedXAssignments.add(fedXAssignment);
        }

        dataset.end();
        dataset.close();

        return fedXAssignments;
    }

    static int countMissingAssignments(List<Map<String, String>> sourceSelection, List<Map<String, String>> optimalSourceSelection) {
        int missingAssignments = 0;
        for (Map<String, String> binding: optimalSourceSelection) {
            boolean found = false;
            for (Map<String, String> otherBinding: sourceSelection) {
                if (otherBinding.entrySet().containsAll(binding.entrySet())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                missingAssignments += 1;
                // return false;
            }
        }
        // return true;
        return missingAssignments;
    }

    protected Map<String, String> bindingToMap(Binding binding) {
        Map<String, String> bindingAsMap = new HashMap<>();
        Iterator<Var> vars = binding.vars();
        while (vars.hasNext()) {
            String varName = vars.next().getName();
            bindingAsMap.put(varName, binding.get(varName).toString());
        }
        return bindingAsMap;
    }

    static List<Map<String, String>> removeInclusions(List<Map<String, String>> sourceSelection) {
        List<Map<String, String>> withoutDuplicates = new ArrayList<>();
        for (Map<String, String> e1 : sourceSelection) {
            if (!(withoutDuplicates.contains(e1))) {
                withoutDuplicates.add(e1);
            }
        }

        List<Map<String, String>> newSourceSelection = new ArrayList<>();
        for (int i = 0; i < withoutDuplicates.size(); i++) {
            boolean keep = true;
            for (int j = 0; j < withoutDuplicates.size(); j++) {
                if (i != j && withoutDuplicates.get(j).entrySet().containsAll(withoutDuplicates.get(i).entrySet())) {
                    keep = false;
                    break;
                }
            }
            if (keep) {
                newSourceSelection.add(withoutDuplicates.get(i));
            }
        }
        return newSourceSelection;
    }

    protected ImmutablePair<Op, List<StatementPattern>> createSourceSelectionQuery(String queryString) throws Exception {
        try {
            // queryString = new TriplePatternsReorderer().optimize(queryString);
            Query query = QueryFactory.create(queryString);
            Op op = Algebra.compile(query);
            ToSourceSelectionTransforms tsst = new ToSourceSelectionTransforms(true, endpoints, ds4Asks);
            op = tsst.transform(op);

            List<StatementPattern> patterns = Utils.getTriplePatterns(queryString);

            Config config = this.connection.getFederation().getConfig();

            Summarizer summarizer = (Summarizer) Util.instantiate(
                    config.getProperty("fedup.summaryClass"),
                    Integer.parseInt(config.getProperty("fedup.summaryArg", "0")));

            // summarizer before toSourceSelectionQuery Transform since it only works on triples
            op = summarizer.summarize(op);
            // op = Transformer.transform(new ToSourceSelectionQueryTransform(), op);

            System.out.println(op.toString());


            return new ImmutablePair<>(op, patterns);
        } catch (Exception e) {
            throw e;
            // throw new Exception("Error when rewriting the query", e.getCause());
        }
    }

    private class TriplePatternsReorderer extends AbstractQueryModelVisitor<Exception> {

        private class StatementPatternWithScore {
            
            private StatementPattern pattern;

            public StatementPatternWithScore(StatementPattern pattern) {
                this.pattern = pattern;
            }

            public StatementPattern getPattern() {
                return this.pattern;
            }

            public int getScore() {
                int score = 0;
                if (this.pattern.getSubjectVar().isConstant()) {
                    score += 4;
                }
                if (this.pattern.getPredicateVar().isConstant() && !this.pattern.getPredicateVar().getValue().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
                    score += 1;
                }
                if (this.pattern.getObjectVar().isConstant()) {
                    score += 2;
                }
                return score;
            }
        }

        private List<StatementPattern> currentBGP = new ArrayList<>();
        private List<String> currentVars = new ArrayList<>();

        private String optimize(String queryString) throws Exception {
            try {
                ParsedQuery parseQuery = new SPARQLParser().parseQuery(queryString, "http://donotcare.com/wathever");
                this.meetNode(parseQuery.getTupleExpr());
                this.reorderBGP();
                return new SPARQLQueryRenderer().render(parseQuery);
            } catch (RDF4JException e) {
                e.printStackTrace();
            }
            return null;
        }

        private boolean isConnected(StatementPattern pattern) {
            return pattern.getVarList().stream().anyMatch(var -> {
                return this.currentVars.contains(var.getName());
            });
        }

        private void updateVars(StatementPattern pattern) {
            List<String> vars = pattern.getVarList().stream().filter(var -> {
                return !var.isConstant();
            }).map(var -> {
                return var.getName();
            }).collect(Collectors.toList());
            this.currentVars.addAll(vars);
        }
                
        private void reorderBGP() throws Exception {
            List<StatementPatternWithScore> scoredTriples = this.currentBGP.stream().map(triple -> {
                return new StatementPatternWithScore(triple);
            }).collect(Collectors.toList());
            
            scoredTriples = scoredTriples.stream().sorted((a, b) -> {
                return b.getScore() - a.getScore();
            }).collect(Collectors.toList());
                        
            List<StatementPattern> orderedTriples = new ArrayList<>();
            while (scoredTriples.size() > 0) {
                boolean cartesianProduct = true;
                for (int i = 0; i < scoredTriples.size(); i++) {
                    if (this.isConnected(scoredTriples.get(i).getPattern())) {
                        this.updateVars(scoredTriples.get(i).getPattern());
                        orderedTriples.add(scoredTriples.remove(i).getPattern());
                        cartesianProduct = false;
                        break;
                    }
                }
                if (cartesianProduct) {
                    this.updateVars(scoredTriples.get(0).getPattern());
                    orderedTriples.add(scoredTriples.remove(0).getPattern());
                }
            }

            for (int i = 0; i < this.currentBGP.size(); i++) {
                this.currentBGP.get(i).replaceWith(orderedTriples.get(i).clone());
            }

            this.currentBGP = new ArrayList<>();
        }

        @Override
        public void meet(Union node) throws Exception {
            node.getLeftArg().visit(this);
            this.reorderBGP();
            this.currentVars = new ArrayList<>();
            node.getRightArg().visit(this);
            this.reorderBGP();
        }

        @Override
        public void meet(LeftJoin node) throws Exception {
            node.getLeftArg().visit(this);
            this.reorderBGP();
            node.getRightArg().visit(this);
            this.reorderBGP();
        }

        @Override
        public void meet(StatementPattern node) throws Exception {
            this.currentBGP.add(node);
        }
    }
}
