package fr.gdd.sage.io;

import fr.gdd.sage.generics.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/**
 * All the data returned by a Sage query execution. Notably, it
 * returns a `state` containing the necessary data to resume the query
 * execution if need be.
 **/
public class SageOutput<SKIP extends Serializable> implements Serializable {

    /**
     * List of variables the values of which are retrieved during query execution.
     */
    List<String> projections   = new ArrayList<>();

    /**
     * The result mappings, each value is mapped to the variable using its index in the array.
     */
    List<List<String>> results = new ArrayList<>();

    /**
     * The number of result mappings.
     */
    long count = 0;

    /**
     * The saved state of query execution. Using this, an engine is able to resume the execution
     * eventually providing completeness.
     */
    TreeMap<Integer, SKIP> state = null;

    TreeMap<Integer, Pair<Long, Long>> progress = new TreeMap<>();

    /* ****************************************************************************************** */

    public SageOutput() {}
    
    public SageOutput(List<String> projections) {
        this.projections = projections;
    }

    public void add(List<String> result) {
        count += 1;
        // In Jena's Volcano, we still use add but the results are already saved elsewhere,
        // so we don't need to add them in the output. Yet, +1 is important to know when to
        // stop.
        if (!Objects.isNull(result)) {
            results.add(result);
        }
    }

    public void addState(Pair<Integer, SKIP> state) {
        if (this.state == null) {
            this.state = new TreeMap<>();
        }
        this.state.put(state.left, state.right);
    }

    @SafeVarargs
    public final void save(Pair<Integer, SKIP>... states) {
        this.state = new TreeMap<>();
        for (Pair<Integer, SKIP> s : states) {
            this.state.put(s.left, s.right);
        }
    }

    public void merge(SageOutput<SKIP> other) {
        this.results.addAll(other.getResults());
        this.count += other.count;
        this.state = other.state;
    }
    
    public List<List<String>> getResults() {
        return results;
    }

    public long size() {
        return count;
    }

    public TreeMap<Integer, SKIP> getState() {
        return state;
    }

    public List<String> getProjections() {
        return projections;
    }

    /**
     * Add the iterator progress to output.
     * @param id The identifier of the iterator that reports its progress.
     * @param offset The current element of the iterator.
     * @param cardinality The number of elements to iterate over.
     */
    public void addProgress(int id, long offset, long cardinality) {
        this.progress.put(id, new Pair<>(offset, cardinality));
        // (TODO)
    }

}
