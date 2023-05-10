package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * Such an iterator can represent optionals in SPARQL queries. When the optional statement does not find
 * any result, the mandatory statement must return nonetheless, with the optional part set as None.
 * Otherwise, it returns results normally.
 *
 * A preemptive version must get a unique identifier to save/resume its execution, i.e. it must remember
 * if up till then, it had found a result for this mandatory statement.
 */
public class PreemptQueryIterDefaulting extends QueryIterDefaulting {

    Integer id;
    SageInput  input;
    SageOutput output;


    public PreemptQueryIterDefaulting(QueryIterator cIter, Binding _defaultObject, ExecutionContext qCxt, Integer id) {
        super(cIter, _defaultObject, qCxt) ;
        this.id = id;
        input  = qCxt.getContext().get(SageConstants.input);
        output = qCxt.getContext().get(SageConstants.output);
    }


    @Override
    protected boolean hasNextBinding() {
        if  (System.currentTimeMillis() >= input.getDeadline() || output.size() >= input.getLimit()) {
            this.output.save(new Pair(id, haveReturnedSomeObject));
            // Need to not return false since iterator will do it,
            // otherwise, it returns an error since it `moveToNextBinding` first then
            // check `hasNextBinding` that returns false…
            // Instead, we empty the iterator by checking all members of union.
            // return false;
        }
        return super.hasNextBinding();
    }


    public void skip(boolean to) {
        haveReturnedSomeObject = to;
    }

}
