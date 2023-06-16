package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.arq.ScanIteratorFactory;
import fr.gdd.sage.configuration.SageInputBuilder;
import fr.gdd.sage.configuration.SageServerConfiguration;
import fr.gdd.sage.io.SageInput;

import org.apache.jena.sparql.algebra.op.OpAssign;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpExt;
import org.apache.jena.sparql.algebra.op.OpExtend;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.*;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.util.Context;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Executes a random branch of the tree that represents a query.
 */
public class OpExecutorRandom extends OpExecutorSage {

    public static class OpExecutorRandomFactory implements OpExecutorFactory {
        SageServerConfiguration configuration;

        public OpExecutorRandomFactory(Context context) {
            configuration = new SageServerConfiguration(context);
        }

        @Override
        public OpExecutor create(ExecutionContext context) {
            return new OpExecutorRandom(context, configuration);
        }
    }

    public OpExecutorRandom(ExecutionContext context, SageServerConfiguration configuration) {
        super(context, configuration);
        ScanIteratorFactory scanFactory = context.getContext().get(SageConstants.scanFactory);
        if (Objects.isNull(scanFactory) || !(scanFactory instanceof RandomScanIteratorFactory)) {
            // since it inherits from Sage, it may be already set to preemptScanIteratorFactory, so we reset
            context.getContext().set(SageConstants.scanFactory, new RandomScanIteratorFactory(context));
        }
    }

    @Override
    public QueryIterator execute(OpUnion union, QueryIterator input) {
        return new RandomQueryIterUnion(input, flattenUnion(union), execCxt);
    }

    @Override
    protected QueryIterator execute(OpJoin opJoin, QueryIterator input) {
        return new RandQueryIterNestedLoopJoin(opJoin, input, execCxt);
    }

    @Override
    protected QueryIterator execute(OpConditional opCondition, QueryIterator input) {
        return new RandomQueryIterOptionalIndex(exec(opCondition.getLeft(), input), opCondition.getRight(), execCxt);
    }
}