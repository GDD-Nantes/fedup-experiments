package fr.gdd.sage.interfaces;



/**
 * An iterator that enables performing random walks in the query.
 */
public interface RandomIterator {

    /**
     * Position the iterator to a random in its allowed
     * range. Therefore, building a random triple only requires to
     * call `it.getId(SPOC.SUBJECT)`, `it.getId(SPOC.PREDICATE)`,
     * `it.getId(SPOC.OBJECT)` to which we add `it.getId(SPOC.GRAPH)`
     * for quads.
     *
     * @return
     */
    boolean random();

    /**
     * @return A -- possibly estimated -- cardinality of the pattern.
     */
    long cardinality();

}
