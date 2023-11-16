package fr.univnantes.gdd.fedup.sourceselection;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.CacheEntry;
import com.fluidops.fedx.cache.CacheUtils;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.exception.ExceptionUtil;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.SubQuery;
import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.Utils;
import fr.univnantes.gdd.fedup.strategies.ModuloOnSuffix;
import fr.univnantes.gdd.fedup.summary.HashSummarizer;
import fr.univnantes.gdd.fedup.summary.RemoveFilterTransformerLol;
import fr.univnantes.gdd.fedup.transforms.Graph2TripleVisitor;
import fr.univnantes.gdd.fedup.transforms.ToSourceSelectionTransforms;
import org.aksw.simba.quetsal.core.TBSSSourceSelection;
import org.apache.jena.base.Sys;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * FedUP with an additional check of found sources using ASK.
 */
public class FedUPFakeASKSSPerformer extends FedUPSourceSelectionPerformer {

    private static final Logger logger = LogManager.getLogger(FedUPFakeASKSSPerformer.class);
    
    Set<String> endpoints;

    public FedUPFakeASKSSPerformer(SailRepositoryConnection connection) throws Exception {
        super(connection);

        // #1 transform the query to get fake ASKs
        // endpoints = this.connection.getEndpoints().stream().map(e -> e.getEndpoint().substring(e.getEndpoint().indexOf("default-graph-uri=") + 18)).collect(Collectors.toSet()); // get graph names from endpoints
        endpoints = this.connection.getEndpoints().stream().map(Endpoint::getEndpoint).collect(Collectors.toSet());
    }

    @Override
    public List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(String queryString) throws Exception {
        Config config = connection.getFederation().getConfig();

        Dataset ds4Asks = null;
        if (Objects.nonNull(config.getProperty("fedup.id"))) {
            ds4Asks = TDB2Factory.connectDataset(config.getProperty("fedup.id"));
        }
        
        // #1 perform fake ASKs on fedup-id to know where triple patterns are
        Integer hashModulo = Integer.parseInt(config.getProperty("fedup.summaryArg"));
        ModuloOnSuffix hs = new ModuloOnSuffix(hashModulo);

        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);
        ToSourceSelectionTransforms tsst = new ToSourceSelectionTransforms(hs, true, endpoints, ds4Asks);
        op = tsst.transform(op);

        // #2 execute the transformed query on the summary
        Dataset dataset = TDB2Factory.connectDataset(config.getProperty("fedup.summary"));
        dataset.begin(ReadWrite.READ);
        QueryEngineTDB.register(); // TODO double check if it reorder or not

        System.out.println(op);

        logger.debug("Executing query...");
        long startTime = System.currentTimeMillis();
        QueryIterator iterator = Algebra.exec(op, dataset);

        List<Map<String, String>> assignments = new ArrayList<>();
        Set<Integer> seen = new TreeSet<>();

        while (iterator.hasNext()) {
            Binding binding = iterator.next();
            int hashcode = binding.toString().hashCode();
            if (!seen.contains(hashcode)) {
                seen.add(hashcode);
                assignments.add(this.bindingToMap(binding));
            }
        }
        long endTime = System.currentTimeMillis();
        Spy.getInstance().sourceSelectionTime = (endTime - startTime);
        logger.debug(String.format("Query execution terminated… Took %s ms", Spy.getInstance().sourceSelectionTime));
        dataset.commit();
        dataset.end();

        assignments = removeInclusions(assignments);

        Spy.getInstance().assignments = assignments;
        Spy.getInstance().numAssignments = assignments.size();

        // #3 Inject sources into the initial query so it can be executed
        Graph2TripleVisitor g2tp = new Graph2TripleVisitor();
        op.visit(g2tp);

        Map<Var, StatementPattern> var2bgp = g2tp.getVar2Triple().entrySet().stream().map(e -> {
            // UGLY AS F
            OpQuad opQuad = new OpQuad(new Quad(e.getKey(), e.getValue()));
            OpQuad opOriginal = (OpQuad) hs.getToOriginal().get(opQuad);
            OpTriple opTriple = new OpTriple(opOriginal.getQuad().asTriple());
            String tripleAsString = OpAsQuery.asQuery(opTriple).toString();
            ParsedQuery parseQuery = new SPARQLParser().parseQuery(tripleAsString, "http://donotcare.com/wathever");
            StatementPattern bgp;
            try {
                List<List<StatementPattern>> bgps = Utils.getBasicGraphPatterns(parseQuery);
                bgp = bgps.get(0).get(0);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            return Map.entry(e.getKey(), bgp);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<Map<StatementPattern, List<StatementSource>>> fedXAssignments = new ArrayList<>();
        for (Map<String, String> assignment: assignments) {
            Map<StatementPattern, List<StatementSource>> fedXAssignment = new HashMap<>();
            for (int i = 1; i <= var2bgp.size(); i++) {
                String alias = "g"+i; // TODO change this
                if (assignment.containsKey(alias)) {
                    Endpoint endpoint = Utils.getEndpointByURL(this.connection.getEndpoints(), assignment.get("g" + i));
                    StatementSource source = new StatementSource(endpoint.getId(), StatementSource.StatementSourceType.REMOTE);
                    StatementPattern pattern = var2bgp.get(Var.alloc(alias));
                    fedXAssignment.put(pattern, List.of(source));
                    Spy.getInstance().tpAliases.put(alias, pattern.toString());
                }
            }
            fedXAssignments.add(fedXAssignment);
        }

        return fedXAssignments;
        // return new UoJvsJoU(this.connection).selectBestAssignment(queryString, fedXAssignments);
    }

}
