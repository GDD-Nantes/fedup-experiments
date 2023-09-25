package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.PreemptIterator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * Such an iterator can represent optionals in SPARQL queries. When the optional statement does not find
 * any result, the mandatory statement must return nonetheless, with the optional part set as None.
 * Otherwise, it returns results normally.
 *
 * A preemptive version must get a unique identifier to save/resume its execution, i.e. it must remember
 * if up till then, it had found a result for this mandatory statement.
 */
public class PreemptQueryIterDefaulting extends QueryIterDefaulting implements PreemptIterator<Boolean> {

    Integer id;

    boolean previous;

    public PreemptQueryIterDefaulting(QueryIterator cIter, Binding _defaultObject, ExecutionContext qCxt, Integer id) {
        super(cIter, _defaultObject, qCxt);
        this.id = id;

        HashMap<Integer, PreemptIterator> iterators = qCxt.getContext().get(SageConstants.iterators);
        iterators.put(id, this);
    }

    @Override
    protected Binding moveToNextBinding() {
        previous = haveReturnedSomeObject;
        return super.moveToNextBinding();
    }

    /* ******************************************************************************** */

    @Override
    public Integer getId() {
        return this.id;
    }

    @Override
    public void skip(Boolean to) {
        super.haveReturnedSomeObject = to;
    }

    @Override
    public Boolean current() {
        return super.haveReturnedSomeObject;
    }

    @Override
    public Boolean previous() {
        return previous;
    }
}
