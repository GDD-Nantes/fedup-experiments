package fr.gdd.sage.optimizer;

import com.github.jsonldjava.utils.Obj;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.jena.JenaBackend;
import org.apache.jena.dboe.trans.bplustree.ProgressJenaIterator;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.shared.NotFoundException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.optimize.VariableUsagePusher;
import org.apache.jena.sparql.algebra.optimize.VariableUsageTracker;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.VarUtils;
import org.apache.jena.tdb2.store.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * It reorders graph patterns using a heuristic based on estimated cardinalities.
 * The topest the smallest; candidates patterns are chosen amongst set variables, all if none.
 */
public class SageOptimizer extends TransformCopy {
    private static Logger log = LoggerFactory.getLogger(SageOptimizer.class);

    JenaBackend backend;
    Dataset dataset;

    VariableUsageTracker alreadySetVars = new VariableUsageTracker();

    public SageOptimizer(Dataset dataset) {
        backend = new JenaBackend(dataset);
        this.dataset = dataset;
    }

    SageOptimizer(Dataset dataset, VariableUsageTracker vars) {
        backend = new JenaBackend(dataset);
        this.dataset = dataset;
        this.alreadySetVars = vars;
    }

    @Override
    public Op transform(OpLeftJoin opLeftJoin, Op left, Op right) {
        VariableUsageTracker tracker = new VariableUsageTracker();
        var variablesVisitor = new VariableUsagePusher(tracker);
        left.visit(variablesVisitor);
        // (TODO) position OPTIONAL in the plan
        // We get the variable set on the left side and inform right side
        return OpLeftJoin.create(left, Transformer.transform(new SageOptimizer(dataset, tracker), right), opLeftJoin.getExprs());
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<Pair<Triple, Double>> tripleToIt = opBGP.getPattern().getList().stream().map(triple -> {
            try {
                NodeId s = triple.getSubject().isVariable() ? backend.any() : backend.getId(triple.getSubject());
                NodeId p = triple.getPredicate().isVariable() ? backend.any() : backend.getId(triple.getPredicate());
                NodeId o = triple.getObject().isVariable() ? backend.any() : backend.getId(triple.getObject());

                BackendIterator<?, ?> it = backend.search(s, p, o);
                ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator<?, ?>) it).iterator;
                log.debug("triple {} => {} elements", triple, casted.cardinality());
                return new Pair<>(triple, casted.cardinality());
            } catch (NotFoundException e) {
                log.debug("triple {} does not exist -> 0 element", triple);
                return new Pair<>(triple, 0.);
            }
        }).sorted((p1, p2) -> { // sort ASC by cardinality
            double c1 = p1.right;
            double c2 = p2.right;
            return Double.compare(c1, c2);
        }).collect(Collectors.toList());

        List<Triple> triples = new ArrayList<>();
        Set<Var> patternVarsScope = new HashSet<>();
        while (tripleToIt.size() > 0) {
            // #A contains at least one variable
            var filtered = tripleToIt.stream().filter(p -> patternVarsScope.stream()
                    .anyMatch(v -> VarUtils.getVars(p.getLeft()).contains(v)) ||
                            VarUtils.getVars(p.getLeft()).stream().anyMatch(v2 -> alreadySetVars.getUsageCount(v2) > 0))
                    .toList();
            if (filtered.isEmpty()) {
                // #B contains none
                filtered = tripleToIt; // everyone is candidate
            }
            Triple toAdd = filtered.get(0).getLeft();
            tripleToIt = tripleToIt.stream().filter(p -> p.getLeft() != toAdd).collect(Collectors.toList());
            VarUtils.addVarsFromTriple(patternVarsScope, toAdd);
            triples.add(toAdd);
        }

        return new OpBGP(BasicPattern.wrap(triples));
    }

    @Override
    public Op transform(OpJoin opJoin, Op left, Op right) {
        List<OpQuad> quads = getAllQuads(opJoin);
        if (Objects.nonNull(quads)) {
            // same as OpBGP with triples , but with quads
            List<Pair<OpQuad, Double>> quadsToIt = quads.stream().map(quad -> {
                try {
                    NodeId g = quad.getQuad().getGraph().isVariable() ? backend.any() : backend.getId(quad.getQuad().getGraph());
                    NodeId s = quad.getQuad().getSubject().isVariable() ? backend.any() : backend.getId(quad.getQuad().getSubject());
                    NodeId p = quad.getQuad().getPredicate().isVariable() ? backend.any() : backend.getId(quad.getQuad().getPredicate());
                    NodeId o = quad.getQuad().getObject().isVariable() ? backend.any() : backend.getId(quad.getQuad().getObject());

                    BackendIterator<?, ?> it = backend.search(s, p, o, g);
                    ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator<?, ?>) it).iterator;
                    log.debug("quad {} => {} elements", quad, casted.cardinality());
                    return new Pair<>(quad, casted.cardinality());
                } catch (NotFoundException e) {
                    return new Pair<>(quad, 0.);
                }
            }).sorted((p1, p2) -> { // sort ASC by cardinality
                double c1 = p1.right;
                double c2 = p2.right;
                return Double.compare(c1, c2);
            }).collect(Collectors.toList());

            List<OpQuad> optimizedQuads = new ArrayList<>();
            Set<Var> patternVarsScope = new HashSet<>();
            while (quadsToIt.size() > 0) {
                // #A contains at least one variable
                var filtered = quadsToIt.stream().filter(p -> patternVarsScope.stream()
                                .anyMatch(v -> VarUtils.getVars(p.getLeft().getQuad().asTriple()).contains(v)) ||
                                VarUtils.getVars(p.getLeft().getQuad().asTriple()) // no `getVarsFromQuad` for some reason
                                        .stream().anyMatch(v2 -> alreadySetVars.getUsageCount(v2) > 0) ||
                                alreadySetVars.getUsageCount(p.getLeft().getQuad().getGraph().toString()) > 0)
                        .toList();
                if (filtered.isEmpty()) {
                    // #B contains none
                    filtered = quadsToIt; // everyone is candidate
                }
                OpQuad toAdd = filtered.get(0).getLeft();
                quadsToIt = quadsToIt.stream().filter(p -> p.getLeft() != toAdd).collect(Collectors.toList());
                VarUtils.addVarsFromQuad(patternVarsScope, toAdd.getQuad());
                optimizedQuads.add(toAdd);
            }

            Op joinedQuads = optimizedQuads.get(0); // at least one
            for (int i = 1; i < quads.size() ; ++i) {
                joinedQuads = OpJoin.create(joinedQuads, optimizedQuads.get(i));
            }

            return joinedQuads;
        } else {
            return super.transform(opJoin, left, right);
        }
    }

    /**
     * Get all quads directly linked together by JOIN operators.
     */
    private static List<OpQuad> getAllQuads(Op op) {
        if (op instanceof OpQuad) {
            List<OpQuad> quads = new ArrayList<>();
            quads.add((OpQuad) op);
            return quads;
        } else if (op instanceof OpJoin) {
            var quads = getAllQuads(((OpJoin) op).getLeft());
            quads.addAll(getAllQuads(((OpJoin) op).getRight()));
            return quads;
        }
        return null;
    }

}
