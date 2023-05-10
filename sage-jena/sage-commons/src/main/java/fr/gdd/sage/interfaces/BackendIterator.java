package fr.gdd.sage.interfaces;



/**
 * An iterator over a backend that enables pausing/resuming query
 * execution. Its internal identifiers are of type `ID`, and it can
 * resume its execution using type `SKIP`.
 */
public interface BackendIterator<ID, SKIP> {

    /**
     * @param code Typically, for basic scan operator, the code would
     * be 0 for subject, 1 for predicate etc.; while for values
     * operator, the code would depend on the variable order.
     * @return The identifier of the variable code.
     */
    ID getId(int code);

    /**
     * Get the value of the variable code. When not implemented, this
     * means that the iterator probably does not have access to the
     * backend dictionary.
     * @param code Same as `getId`.
     * @return The value of the variable code.
     */
    default String getValue(int code){return null;};
    
    /**
     * @return true if there are other elements matching the pattern,
     * false otherwise.
     */
    public boolean hasNext();

    /**
     * Iterates to the next element.
     */
    public void next();
    
    /**
     * Go back to the begining of the iterator. Enables reusing of
     * iterators.
     */
    public void reset();

    /**
     * Goes to the targeted element directly.
     * @param to The cursor location to skip to.
     */
    public void skip(final SKIP to);
    
    /**
     * @return The current offset that allows skipping.
     */
    public SKIP current();
    
    /**
     * @return The previous offset that allows skipping.
     */
    public SKIP previous();
}
