package fr.univnantes.gdd.fedup.startup;

import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.exception.RuntimeInterruptedException;
import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.sourceselection.SACost;
import fr.univnantes.gdd.fedup.sourceselection.SourceAssignmentsSingleton;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FedUPQueryExecutor {
    
    private final SailRepositoryConnection connection; // is connection thread safe??

    public FedUPQueryExecutor(SailRepositoryConnection connection) {
        this.connection = connection;
    }

    public void execute(String queryString, List<Map<StatementPattern, List<StatementSource>>> assignments) throws Exception {
        List<Integer> costs = SACost.compute((FedXConnection) this.connection.getSailConnection(), queryString, assignments);

        List<Map<StatementPattern, List<StatementSource>>> sortedAssignments = IntStream.range(0, assignments.size())
                .boxed()
                .sorted(Comparator.comparingInt(costs::get))
                .map(assignments::get)
                .toList();

        SourceAssignmentsSingleton.getInstance().setAssignments(sortedAssignments);

        Query query = QueryFactory.create(queryString);

        ResultsManager resultsManager = new ResultsManager(assignments.size(), query);

        List<Future<?>> futures = new ArrayList<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(Math.max(Math.min(assignments.size(), 8), 1))) {
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < assignments.size(); i++) {
                Future<?> future = executor.submit(() -> {
                    try {
                        if (!resultsManager.isLimitReached()) {
                            TupleQuery q = this.connection.prepareTupleQuery(queryString);
                            TupleQueryResult results = q.evaluate();
                            while (results.hasNext()) {
                                if (resultsManager.addSolution(results.next())) {
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        if (!(e.getMessage().toLowerCase().contains("interrupt"))
                                && !(e.getCause().getMessage().toLowerCase().contains("request aborted"))
                                && !(e instanceof RuntimeInterruptedException)) {
                            e.printStackTrace(); // print only exceptions that do not come from cancelling query execution tasks
                        }
                    } finally {
                        resultsManager.notifyComplete();
                    }
                });
                futures.add(future);
            }
            resultsManager.waitForResults();
            futures.forEach(f -> f.cancel(true));
            long endTime = System.currentTimeMillis(); // not outside the try statement to avoid counting FedX shutdown time in query execution time

            Spy.getInstance().solutions = resultsManager.getSolutions();
            Spy.getInstance().numSolutions += resultsManager.size();
            Spy.getInstance().executionTime = endTime - startTime;
        }
    }

    private class ResultsManager {

        private int remainingProducers;
        private long limit = Long.MAX_VALUE;
        private MultiSet<BindingSet> bindings = new HashMultiSet<>();

        HasOptionalVisitor hasOptional = new HasOptionalVisitor();
        HasOrderByVisitor hasOrderBy = new HasOrderByVisitor();
        Query query;
        Op op;

        // number of solutions that have their OPTIONAL part complete, ie,
        // bindings match the OPTIONAL part. Instead of waiting for every results
        // it can directly trigger the LIMIT reached.
        Integer completeSolutions = 0;
        long limitOptionalOrOrderBy = Long.MAX_VALUE;
        ActualVarsVisitor actualVars = new ActualVarsVisitor();
        List<Var> projectedVars;

        public ResultsManager(Integer remainingProducers, Query query) {
            this.query = query;
            op = Algebra.compile(query);
            op.visit(hasOptional);

            // remove projected that are useless
            // important to make sure result bindings arrive complete
            projectedVars = new ArrayList<>(query.getProjectVars());
            op.visit(actualVars);
            if (Objects.nonNull(actualVars.vars)) {
                projectedVars.retainAll(actualVars.vars);
            }

            if (query.hasLimit()) {
                // because post-process is need to remove
                // included bindings and/or reorder merged results
                limit = (query.hasOrderBy() || hasOptional.result) ? Long.MAX_VALUE : query.getLimit();
                limitOptionalOrOrderBy = (query.hasOrderBy() || hasOptional.result) ? query.getLimit() : Long.MAX_VALUE;
            }
            if (query.hasOrderBy()) {
                op.visit(hasOrderBy);
            }

            this.remainingProducers = remainingProducers;
        }

        public synchronized boolean isLimitReached() {
            return this.size() >= this.limit || this.completeSolutions >= this.limitOptionalOrOrderBy;
        }

        public void addProducer() {
            this.remainingProducers += 1;
        }

        public synchronized void waitForResults() {
            while (this.size() < this.limit
                    && this.completeSolutions < this.limitOptionalOrOrderBy
                    && this.remainingProducers > 0) {
                try {
                    wait();
                } catch (InterruptedException ignored) { }
            }
        }

        public synchronized Integer size() {
            // different depending on whether it's a DISTINCT or not
            return query.isDistinct() ? bindings.uniqueSet().size() : bindings.size();
        }

        public synchronized boolean addSolution(BindingSet solution) {
            if (this.size() >= this.limit || this.completeSolutions >= this.limitOptionalOrOrderBy) { // to avoid threads from adding new results before cancel when the limit is reached
                return true;
            }
            if (hasAllVarsSet(projectedVars, solution)) {
                if (query.isDistinct()) {
                    if (!this.bindings.contains(solution)) {
                        completeSolutions += 1;
                    }
                } else {
                    completeSolutions += 1;
                }
            }
            if (hasOptional.result) {
                // normally, it would require a clever solution based on
                // the query plan to determine the optional variables and
                // their respective provenance. For our specific case, this
                // basic inclusion check works, although not efficient.
                removeStrictInclusionsAndAdd(solution);
            } else {
                this.bindings.add(solution);
            }
            return this.size() >= this.limit || this.completeSolutions >= this.limitOptionalOrOrderBy ;
        }

        public synchronized void notifyComplete() {
            this.remainingProducers -= 1;
            notifyAll();
        }

        public List<String> getSolutions() {
            Stream<BindingSet> result = query.isDistinct() ? bindings.uniqueSet().stream() : bindings.stream();
            if (query.hasOrderBy()) { // ugly !
                // post-process ORDER BY when we have all results needed
                result = result.sorted((a, b) -> {
                    for (SortCondition sc : hasOrderBy.result.getConditions()) {
                        Var v = sc.expression.asVar();
                        int compared = a.getValue(v.getVarName()).stringValue()
                                .compareTo(b.getValue(v.getVarName()).stringValue());
                        if (compared != 0) {
                            return compared;
                        }
                    }
                    return 0;
                });
            }
            return result.map(BindingSet::toString).toList();
        }

        public void removeStrictInclusionsAndAdd(BindingSet solution) {
            // #0 check if solution exists in bindings
            // should be efficient to discard full duplicates
            if (bindings.contains(solution)) {
                bindings.add(solution);
                return; // already checked from previous iteration
            }

            List<BindingSet> toRemove = new ArrayList<>();
            for (BindingSet binding: this.bindings.uniqueSet()) {
                // #1 check if binding included in solution
                if (isIncluded(binding, solution)) {
                    toRemove.add(binding);
                }
                // #2 check if solution included in binding
                if (isIncluded(solution, binding)) {
                    toRemove.add(solution);
                }
            }

            this.bindings.add(solution); // will be removed if need be
            this.bindings.removeAll(toRemove);
        }


        public static boolean isIncluded(BindingSet included, BindingSet base) {
            Set<String> baseVars = new HashSet<>(base.getBindingNames());
            Set<String> includedVars = new HashSet<>(included.getBindingNames());

            if (!baseVars.containsAll(included.getBindingNames())) { // all base vars are not in included
                return false;
            }

            includedVars.removeAll(baseVars);
            if (!includedVars.isEmpty()) { // included has its own variables
                return false;
            }

            // check the values
            for (Binding binding : included) {
                // as soon as a value is not equal, return false
                if (!Objects.equals(base.getBinding(binding.getName()).getValue(), binding.getValue())) {
                    return false;
                }
            }
            return true;
        }

        public static boolean hasAllVarsSet(List<Var> projected, BindingSet bindings) {
            return projected.stream().allMatch(p -> bindings.hasBinding(p.getName()));
        }

    }
}
