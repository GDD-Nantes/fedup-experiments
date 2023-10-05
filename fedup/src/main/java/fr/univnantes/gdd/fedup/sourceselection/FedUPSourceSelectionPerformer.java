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
import fr.univnantes.gdd.fedup.strategies.Identity;
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
import org.apache.zookeeper.data.Id;
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

    private static final Logger logger = LogManager.getLogger(FedUPSourceSelectionPerformer.class);

    Set<String> endpoints;
    Dataset ds4Asks;

    public FedUPSourceSelectionPerformer(SailRepositoryConnection connection) throws Exception {
        super(connection);

        // #1 transform the query to get fake ASKs
        endpoints = this.connection.getEndpoints().stream().map(e -> e.getEndpoint().substring(e.getEndpoint().indexOf("default-graph-uri=") + 18)).collect(Collectors.toSet()); // get graph names from endpoints
        ds4Asks = TDB2Factory.connectDataset(this.connection.getFederation().getConfig().getProperty("fedup.summary"));
    }

    @Override
    public List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(
        String queryString, List<Map<String, String>> optimalAssignments
    ) throws Exception {
        Config config = this.connection.getFederation().getConfig();

        long timeout = Long.parseLong(config.getProperty("fedup.budget", "0"));

        Dataset dataset = TDB2Factory.connectDataset(config.getProperty("fedup.summary"));
        dataset.begin(TxnType.READ);

        QueryEngineTDB.register();
        QueryEngineFactory factory = QueryEngineTDB.getFactory();

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
            if (!seen.contains(hashcode)) {
                seen.add(hashcode);
                assignments.add(this.bindingToMap(binding));
            }
        }
        long endTime = System.currentTimeMillis();
        logger.debug("Query execution terminated...");

        Spy.getInstance().sourceSelectionTime = (endTime - startTime);

        assignments = removeInclusions(assignments);

        Spy.getInstance().assignments = assignments;
        Spy.getInstance().numAssignments = assignments.size();

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
                    Spy.getInstance().tpAliases.put(alias, pattern.toString());
                }
            }
            fedXAssignments.add(fedXAssignment);
        }
        dataset.commit();
        dataset.end();

        return fedXAssignments;
        // return new UoJvsJoU(this.connection).selectBestAssignment(queryString, fedXAssignments);
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
        Identity id = new Identity();

        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);
        ToSourceSelectionTransforms tsst = new ToSourceSelectionTransforms(id, true, endpoints, ds4Asks);
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
    }
}