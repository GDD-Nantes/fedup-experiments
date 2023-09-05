package fr.univnantes.gdd.fedup.sourceselection;

import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.CacheEntry;
import com.fluidops.fedx.cache.CacheUtils;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.exception.ExceptionUtil;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.structures.*;
import fr.univnantes.gdd.fedup.Spy;
import org.aksw.simba.quetsal.core.TBSSSourceSelection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * FedUP with an additional check of found sources using ASK.
 */
public class FedUPAskSourceSelectionPerformer extends FedUPSourceSelectionPerformer {

    private static Logger log = LogManager.getLogger(FedUPAskSourceSelectionPerformer.class);

    public FedUPAskSourceSelectionPerformer(SailRepositoryConnection connection) throws Exception {
        super(connection);
    }

    @Override
    public List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(String queryString, List<Map<String, String>> optimalAssignments, Spy spy) throws Exception {
        // We are guided by our summary, so we first perform random walks on it
        var withoutAsk = super.performSourceSelection(queryString, optimalAssignments, spy);

        // then we remove sources that do not actually contribute by performing ASK queries
        List<Endpoint> endpoints = connection.getEndpoints();
        Map<String, Endpoint> id2Endpoint = new HashMap<>(); // for convenience
        for (Endpoint endpoint : endpoints) {
            id2Endpoint.put(endpoint.getId(), endpoint);
        }

        // keep unique pairs
        Set<Pair<Integer, String>> toCheck = new HashSet<>(); // pattern + source
        for (Map<StatementPattern, List<StatementSource>> row : withoutAsk) {
            for (Map.Entry<StatementPattern, List<StatementSource>> entry : row.entrySet()) {
                for (StatementSource source : entry.getValue()) {
                    toCheck.add(new Pair<>(entry.getKey().hashCode(), source.getEndpointID()));
                }
            }
        }

        // register unique pairs
        List<MeowSourceSelection.CheckTaskPair> remoteCheckTasks = new ArrayList<>();
        for (Map<StatementPattern, List<StatementSource>> row : withoutAsk) {
            for (Map.Entry<StatementPattern, List<StatementSource>> entry : row.entrySet()) {
                for (StatementSource source : entry.getValue()) {
                    var patternAndSource = new Pair<>(entry.getKey().hashCode(), source.getEndpointID());
                    if (toCheck.contains(patternAndSource)) {
                        toCheck.remove(patternAndSource);
                        remoteCheckTasks.add(new MeowSourceSelection.CheckTaskPair(
                                   id2Endpoint.get(source.getEndpointID()),
                                   entry.getKey()));
                    }
                }
            }
        }

        // perform ask on statements and their respective processed sources
        QueryInfo queryInfo = new QueryInfo(connection, queryString, QueryType.SELECT, null);
        var meow = new MeowSourceSelection(endpoints, connection.getFederation().getCache(), queryInfo, spy);

        MeowSourceSelection.SourceSelectionExecutorWithLatch.run(
                queryInfo.getFederation().getScheduler(), meow,
                remoteCheckTasks, connection.getFederation().getCache());

        var stmt2Sources = meow.getStmtToSources();

        // remove rows of statements that supposed a source existed while it does not
        // var withAsk = new ArrayList<Map<StatementPattern, List<StatementSource>>>();
        for (int i = 0; i < withoutAsk.size(); ++i) {
            // boolean toRemove = false;
            for (Map.Entry<StatementPattern, List<StatementSource>> entry : withoutAsk.get(i).entrySet()) {
                if (!(stmt2Sources.containsKey(entry.getKey()) &&
                        stmt2Sources.get(entry.getKey()).containsAll(entry.getValue()))) {
                    withoutAsk.get(i).remove(entry.getKey());
                    i -= 1;
                }
            }
            // if (!toRemove) {
            //     withAsk.add(withoutAsk.get(i));
            // }
        }


        return withoutAsk; // is actually with ASK now
    }


    public class MeowSourceSelection extends TBSSSourceSelection {
        private Spy spy;

        public MeowSourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo, Spy spy) {
            super(endpoints, cache, queryInfo);
            this.spy = spy;
        }

        public static class SourceSelectionExecutorWithLatch {

            /**
             * Execute the given list of tasks in parallel, and block the thread until
             * all tasks are completed. Synchronization is achieved by means of a latch.
             * Results are added to the map of the source selection instance. Errors
             * are reported as {@link OptimizationException} instances.
             *
             * @param hibiscusSourceSelection Quetsal Source Selection
             * @param tasks                   Set of SPARQL ASK tasks
             * @param cache                   Cache
             */
            public static void run(ControlledWorkerScheduler scheduler, MeowSourceSelection hibiscusSourceSelection, List<CheckTaskPair> tasks, Cache cache) {
                new SourceSelectionExecutorWithLatch(scheduler, hibiscusSourceSelection).executeRemoteSourceSelection(tasks, cache);
            }

            private final MeowSourceSelection sourceSelection;
            private final ControlledWorkerScheduler scheduler;

            private SourceSelectionExecutorWithLatch(ControlledWorkerScheduler scheduler, MeowSourceSelection hibiscusSourceSelection) {
                this.scheduler = scheduler;
                this.sourceSelection = hibiscusSourceSelection;
            }

            /**
             * Execute the given list of tasks in parallel, and block the thread until
             * all tasks are completed. Synchronization is achieved by means of a latch
             *
             * @param tasks
             */
            private void executeRemoteSourceSelection(List<CheckTaskPair> tasks, Cache cache) {
                if (tasks.isEmpty()) {
                    return;
                }

                List<Exception> errors = new ArrayList<Exception>();
                List<Future<Void>> futures = new ArrayList<Future<Void>>();
                for (CheckTaskPair task : tasks) {
                    futures.add(scheduler.schedule(new ParallelCheckTask(task.e, task.t, sourceSelection), QueryInfo.getPriority() + 1));
                }

                for (Future<Void> future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        log.debug("Error during source selection. Thread got interrupted.");
                        break;
                    } catch (Exception e) {
                        errors.add(e);
                    }
                }

                if (!errors.isEmpty()) {
                    log.error(errors.size() + " errors were reported:");
                    for (Exception e : errors) {
                        log.error(ExceptionUtil.getExceptionString("Error occured", e));
                    }
                    Exception ex = errors.get(0);
                    errors.clear();
                    if (ex instanceof OptimizationException) {
                        throw (OptimizationException) ex;
                    }
                    throw new OptimizationException(ex.getMessage(), ex);
                }
            }
        }


        public static class CheckTaskPair {
            public final Endpoint e;
            public final StatementPattern t;

            public CheckTaskPair(Endpoint e, StatementPattern t) {
                this.e = e;
                this.t = t;
            }
        }


        /**
         * Task for sending an ASK request to the endpoints (for source selection)
         *
         * @author Andreas Schwarte
         */
        protected static class ParallelCheckTask implements Callable<Void> {

            final Endpoint endpoint;
            final StatementPattern stmt;
            final MeowSourceSelection sourceSelection;

            public ParallelCheckTask(Endpoint endpoint, StatementPattern stmt, MeowSourceSelection sourceSelection) {
                this.endpoint = endpoint;
                this.stmt = stmt;
                this.sourceSelection = sourceSelection;
            }


            @Override
            public Void call() throws Exception {
                try {
                    TripleSource t = endpoint.getTripleSource();
                    RepositoryConnection conn = endpoint.getConn();

                    boolean hasResults = t.hasStatements(stmt, conn, EmptyBindingSet.getInstance());

                    CacheEntry entry = CacheUtils.createCacheEntry(endpoint, hasResults);
                    sourceSelection.cache.updateEntry(new SubQuery(stmt), entry);

                    if (hasResults) {
                        sourceSelection.addSource(stmt, new StatementSource(endpoint.getId(), StatementSource.StatementSourceType.REMOTE));
                    }

                    return null;
                } catch (Exception e) {
                    throw new OptimizationException("Error checking results for endpoint " + endpoint.getId() + ": " + e.getMessage(), e);
                }
            }
        }
    }
}
