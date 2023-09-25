package fr.gdd.sage.interfaces;

import java.io.Serializable;

public interface PreemptIterator<SKIP extends Serializable> {

    /**
     * @return The unique identifier of the iterator.
     */
    Integer getId();


    /**
     * Goes to the targeted element directly.
     * @param to The cursor location to skip to.
     */
    void skip(final SKIP to);

    /**
     * @return The current offset that allows skipping.
     */
    SKIP current();

    /**
     * @return The previous offset that allows skipping.
     */
    SKIP previous();

}
