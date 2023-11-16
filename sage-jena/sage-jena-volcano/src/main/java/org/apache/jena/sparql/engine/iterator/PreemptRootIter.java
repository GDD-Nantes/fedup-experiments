package org.apache.jena.sparql.engine.iterator;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;

/**
 * The sole purpose of this iterator is to catch exception thrown by the underlying
 * iterators.
 */
public class PreemptRootIter implements QueryIterator {

    Binding buffered;
    QueryIterator wrapped;

    boolean consumed = true;
    boolean doesHaveNext = false;

    ExecutionContext context;

    public PreemptRootIter(QueryIterator cIter, ExecutionContext context) {
        wrapped = cIter;
        this.context = context;
    }

    @Override
    public boolean hasNext() {
        if (!consumed) {
            return doesHaveNext;
        }

        try {
            doesHaveNext = wrapped.hasNext();
        } catch (PauseException e) {
            close();
            return false;
        }


        if (doesHaveNext) {
            try {
                buffered = wrapped.next();
                // may save during the `.next()` which would set `.hasNext()` as false while
                // it expects and checks `true`. When it happens, it throws a `NoSuchElementException`
            } catch (PauseException e) {
                close();
                return false;
            }
            consumed = false;
            return true;
        }
        return false;
    }

    @Override
    public Binding next() {
        consumed = true;
        return buffered;
    }

    @Override
    public Binding nextBinding() {
        return next();
    }


    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public void cancel() {
        wrapped.cancel();
    }

    @Override
    public void output(IndentedWriter out, SerializationContext sCxt) {

    }

    @Override
    public void output(IndentedWriter out) {

    }

    @Override
    public String toString(PrefixMapping pmap) {
        return null;
    }
}
