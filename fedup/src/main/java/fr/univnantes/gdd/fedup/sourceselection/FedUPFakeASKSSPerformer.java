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
import fr.univnantes.gdd.fedup.transforms.ToSourceSelectionTransforms;
import org.aksw.simba.quetsal.core.TBSSSourceSelection;
import org.apache.jena.base.Sys;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
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

    private static Logger logger = LogManager.getLogger(FedUPFakeASKSSPerformer.class);

    SailRepositoryConnection sailRepositoryConnection;

    Set<String> endpoints;
    Dataset ds4Asks;


    public FedUPFakeASKSSPerformer(SailRepositoryConnection connection) throws Exception {
        super(connection);
        this.sailRepositoryConnection = connection;

        // #1 transform the query to get fake ASKs
        endpoints = this.connection.getEndpoints().stream().map(e ->
                e.getEndpoint().substring(e.getEndpoint().indexOf("default-graph-uri=") + 18,
                        e.getEndpoint().length())).collect(Collectors.toSet()); // get graph names from endpoints
        ds4Asks = TDB2Factory.connectDataset(this.connection.getFederation().getConfig().getProperty("fedup.id"));
    }

    @Override
    public List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(String queryString, List<Map<String, String>> optimalAssignments, Spy spy) throws Exception {
        Config config = connection.getFederation().getConfig();

        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);
        ToSourceSelectionTransforms tsst = new ToSourceSelectionTransforms(true, endpoints, ds4Asks);
        op = tsst.transform(op);
        // spy.numASKQueries = tsst.getNBASKs TODO if need be


        // #2 execute the transformed query on the summary
        Integer hashModulo = Integer.parseInt(config.getProperty("fedup.summaryArg"));
        ModuloOnSuffix hs = new ModuloOnSuffix(hashModulo);
        op = Transformer.transform(hs, op);

        Dataset dataset = TDB2Factory.connectDataset(config.getProperty("fedup.summary"));
        // Dataset dataset = TDB2Factory.connectDataset(config.getProperty("fedup.id"));
        dataset.begin(ReadWrite.READ);

        QueryEngineTDB.register(); // TODO double check if it reorder or not

        /* Query queryTemp = QueryFactory.create("SELECT * WHERE { GRAPH ?g {?s <http://www.w3.org/2002/07/owl#sameAs> <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product171547>}}");
        Op opTemp = Algebra.compile(queryTemp);
        QueryIterator iteratorTemp = Algebra.exec(opTemp, dataset);
        while (iteratorTemp.hasNext()) {
            System.out.println(iteratorTemp.next().toString());
        }*/

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
        spy.sourceSelectionTime = (endTime - startTime);
        logger.debug(String.format("Query execution terminated… Took %s ms", spy.sourceSelectionTime));
        dataset.end();
        dataset.close();

        assignments = this.removeInclusions(assignments);

        spy.assignments = assignments;
        spy.numAssignments = assignments.size();
        spy.numValidAssignments = optimalAssignments.size();
        spy.numFoundAssignments = optimalAssignments.size() - this.countMissingAssignments(assignments, optimalAssignments);


        // #3 Inject sources into the initial query so it can be executed
        List<StatementPattern> patterns = Utils.getTriplePatterns(queryString);
        List<Map<StatementPattern, List<StatementSource>>> fedXAssignments = new ArrayList<>();

        for (Map<String, String> assignment: assignments) {
            Map<StatementPattern, List<StatementSource>> fedXAssignment = new HashMap<>();
            for (int i = 1; i <= patterns.size(); i++) {
                String alias = "g"+i;
                if (assignment.containsKey(alias)) {
                    Endpoint endpoint = Utils.getEndpointByURL(this.connection.getEndpoints(), assignment.get("g" + i));
                    StatementSource source = new StatementSource(endpoint.getId(), StatementSource.StatementSourceType.REMOTE);
                    StatementPattern pattern = patterns.get(i - 1);
                    fedXAssignment.put(pattern, List.of(source));
                    spy.tpAliases.put(alias, pattern.toString());
                }
            }
            fedXAssignments.add(fedXAssignment);
        }


        return fedXAssignments;
    }


}
