package fr.univnantes.gdd.fedup.transforms;

import fr.gdd.raw.io.OpVisitorUnimplemented;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Var;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility function that visits an Op and register the graph variable of each triple.
 */
public class Graph2TripleVisitor extends OpVisitorUnimplemented {

    Map<Var, Triple> var2Triple = new HashMap<>();
    Map<Triple, Var> triple2Var = new HashMap<>();

    public Map<Triple, Var> getTriple2Var() {
        return triple2Var;
    }

    public Map<Var, Triple> getVar2Triple() {
        return var2Triple;
    }

    /* ************************************************************ */

    @Override
    public void visit(OpQuad opQuad) {
        Node graph = opQuad.getQuad().getGraph();
        Triple triple = opQuad.getQuad().asTriple();
        if (opQuad.getQuad().getGraph() instanceof Var) {
            var2Triple.put((Var) graph, triple);
            triple2Var.put(triple, (Var) graph);
        }
    }

    @Override
    public void visit(OpBGP opBGP) {
        // nothing, maybe change if in subOp of OpGraph
    }

    @Override
    public void visit(OpFilter opFilter) {
        opFilter.getSubOp().visit(this);
    }

    @Override
    public void visit(OpSlice opSlice) {
        opSlice.getSubOp().visit(this);
    }

    @Override
    public void visit(OpProject opProject) {
        opProject.getSubOp().visit(this);
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        opLeftJoin.getLeft().visit(this);
        opLeftJoin.getRight().visit(this);
    }

    @Override
    public void visit(OpJoin opJoin) {
        opJoin.getLeft().visit(this);
        opJoin.getRight().visit(this);
    }

    @Override
    public void visit(OpUnion opUnion) {
        opUnion.getLeft().visit(this);
        opUnion.getRight().visit(this);
    }

    @Override
    public void visit(OpConditional opCondition) {
        opCondition.getLeft().visit(this);
        opCondition.getRight().visit(this);
    }

    @Override
    public void visit(OpSequence opSequence) {
        for (Op op : opSequence.getElements()) {
            op.visit(this);
        }
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        opDistinct.getSubOp().visit(this);
    }

    public void visit(OpTable opTable) {
        // nothing
    }
}
