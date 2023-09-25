package fr.gdd.sage.interfaces;

import java.io.Serializable;

import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;

/**
 * Calling `execute` with the proper `sageInput` to execute the query
 * that may pause itself during execution.

 **/
public interface Executor<SKIP extends Serializable> {

    /**
     * @return The - possibly partial - result of the query along with
     * metadata when resuming is possible.
     */
    SageOutput<SKIP> execute(SageInput<SKIP> sageInput);

}
