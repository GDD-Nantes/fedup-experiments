package org.apache.jena.sparql.engine.iterator;

import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;

/**
 * A nested loop join operator. In the {@link PreemptQueryIterNestedLoopJoin}, the iterator saves the results of
 * the left hand side as it goes.
 *
 * In a preemptive version, such state is not allowed. Therefore, every iterator
 * must be recreated when needed.
 */
public class PreemptQueryIterNestedLoopJoin extends QueryIter {

    Op leftOp;
    Op rightOp;

    QueryIterator input;
    QueryIterator leftInput = null;
    QueryIterator rightInput = null;

    Binding rowRight = null;

    private Binding slot     = null;
    private boolean finished = false;

    public PreemptQueryIterNestedLoopJoin(OpJoin opJoin, QueryIterator input, ExecutionContext context) {
        super(context);
        this.input = input;
        leftOp = opJoin.getLeft();
        rightOp = opJoin.getRight();
    }

    @Override
    protected boolean hasNextBinding() {
        if ( finished )
            return false;
        if ( slot == null ) {
            slot = moveToNextBindingOrNull();
            if ( slot == null ) {
                close();
                return false;
            }
        }
        return true;
    }

    @Override
    protected Binding moveToNextBinding() {
        Binding r = slot;
        slot = null;
        return r;
    }


    protected Binding moveToNextBindingOrNull() {
        if ( isFinished() )
            return null;

        for ( ;; ) { // For rows from the right.
            if ( rowRight == null ) {
                rightInput = QC.execute(rightOp, input, getExecContext());
                if ( rightInput.hasNext() ) {
                    rowRight = rightInput.next();
                    leftInput = QC.execute(leftOp, QueryIterRoot.create(getExecContext()), getExecContext());
                } else
                    return null;
            }

            // There is a rowRight
            while (leftInput.hasNext()) {
                Binding rowLeft = leftInput.next();
                Binding r = Algebra.merge(rowLeft, rowRight);
                if ( r != null ) {
                    return r;
                }
            }
            // Nothing more for this rowRight.
            performClose(rightInput);
            performClose(leftInput);
            rowRight = null;
        }
    }


    @Override
    protected void closeIterator() {
        closeSubIterator() ;
        performClose(leftInput) ;
        performClose(rightInput) ;
        leftInput = null ;
        rightInput = null ;
    }

    @Override
    protected void requestCancel() {
        requestSubCancel() ;
        performRequestCancel(leftInput) ;
        performRequestCancel(rightInput) ;
    }

    protected void requestSubCancel() {}
    protected void closeSubIterator() {}

}
