package org.aksw.simba.quetsal.core.evaluation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;

import com.fluidops.fedx.algebra.BoundJoinTupleExpr;
import com.fluidops.fedx.algebra.FedXService;
import com.fluidops.fedx.evaluation.FederationEvalStrategy;
import com.fluidops.fedx.evaluation.concurrent.Async;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.evaluation.iterator.QueueIteration;
import com.fluidops.fedx.evaluation.iterator.RestartableCloseableIteration;
import com.fluidops.fedx.evaluation.iterator.RestartableLookAheadIteration;
import com.fluidops.fedx.structures.QueryInfo;

public class BindJoinImpl extends RestartableLookAheadIteration<BindingSet> {
	public static Logger log = LoggerFactory.getLogger(BindJoinImpl.class);
	
	final ControlledWorkerScheduler scheduler;
	protected final FederationEvalStrategy strategy;		// the evaluation strategy
	protected final TupleExpr rightArg;						// the right argument for the join
	protected final BindingSet ebindings;					// the bindings
	final QueryInfo queryInfo;
	
	int leftCount = 0;
	int rightCount = 0;
	
	protected CloseableIteration<BindingSet, QueryEvaluationException> leftIter;
	
	private QueueIteration<BindingSet> iteration = new QueueIteration<BindingSet>();
	
	static boolean hasFreeVar(BindingSet left, Set<String> right) {
		for (String r : right) {
			if (!left.hasBinding(r)) return true;
		}
		return false;
	}
	
	public BindJoinImpl(ControlledWorkerScheduler scheduler, FederationEvalStrategy strategy,
			TupleExpr leftArg, TupleExpr rightArg, BindingSet ebindings, QueryInfo queryInfo)
	{
		this.scheduler = scheduler;
		this.strategy = strategy;
		this.leftIter = strategy.evaluate(leftArg, ebindings);
		this.rightArg = rightArg;
		this.ebindings = ebindings;
		this.queryInfo = queryInfo;
		
		scheduler.schedule(new AsyncBindingTask());
	}

	private boolean canApplyVectoredEvaluation(TupleExpr expr) {
		if (expr instanceof BoundJoinTupleExpr) {
			if (expr instanceof FedXService) 
				return queryInfo.getFederation().getConfig().getEnableServiceAsBoundJoin();
			return true;
		}				
		return false;
	}
	
	protected void handleBindings() {
		if (!canApplyVectoredEvaluation(rightArg)) { // Exclusive Group
			while (!isClosed() && leftIter.hasNext()) {
				BindingSet b = leftIter.next();
				strategy.evaluate(iteration, rightArg, Arrays.asList(b));
			}
			return;
		}
		
		boolean hasLeftProducedResults = false;
		int nBindingsCfg = queryInfo.getFederation().getConfig().getBoundJoinBlockSize();
		while (true) {
			List<BindingSet> bindings = new ArrayList<BindingSet>(nBindingsCfg);
			while (!isClosed() && bindings.size() < nBindingsCfg && leftIter.hasNext()) {
				BindingSet b = leftIter.next();
				bindings.add(b);
				leftCount++;
			}
			if (isClosed() || bindings.isEmpty()) break;
			strategy.evaluate(iteration, rightArg, bindings);
			hasLeftProducedResults = true;
		}
		if (!hasLeftProducedResults) {
			strategy.evaluate(iteration, rightArg, null); // use cached bindings for new right sources
		}
	}
	
	@Override
	protected BindingSet getNextElement() {
		if (iteration.hasNext()) {
			rightCount++;
			return iteration.next();
		}
		return null;
	}

	@Override
	public void handleClose() {
		log.info("on close: left=" + leftCount + ", right = " + rightCount);
		iteration.close();
	}
	
	@Override
	public void handleRestart() {
		iteration.restart();
		if (leftIter instanceof RestartableCloseableIteration) {
			((RestartableCloseableIteration<BindingSet>) leftIter).restart();
		}
		scheduler.schedule(new AsyncBindingTask());
	}
	
	public class AsyncBindingTask extends Async<Void> {
		public AsyncBindingTask() {
			super(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					return null;
				}
			});
			iteration.getResultQueue().onAddIterator(); // pin
		}
		
		@Override
		public void callAsync(Void val) {
			try {
				handleBindings();
			} finally {
				iteration.getResultQueue().onRemoveIterator(); // unpin
			}
		}

		@Override
		public void exception(Exception e) {
			throw new RuntimeException("logic error"); // must not be ever called
		}
		
		public void cancel() {
			iteration.getResultQueue().onRemoveIterator();
			close();
		}
	}
}
