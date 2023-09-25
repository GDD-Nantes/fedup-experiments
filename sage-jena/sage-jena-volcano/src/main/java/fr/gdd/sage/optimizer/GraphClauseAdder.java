package fr.gdd.sage.optimizer;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;

import java.util.HashMap;

/**
 * Add clause graphs to triple patterns to retrieve the sources of matching triples.
 */
public class GraphClauseAdder extends TransformCopy {

    HashMap<Var, Triple> varToTriple = new HashMap<>();
    HashMap<Triple, Var> tripleToVar = new HashMap<>();

    @Override
    public Op transform(OpBGP opBGP) {
        var quads = opBGP.getPattern().getList().stream().map(triple -> {
            Var v = getNewVar();
            varToTriple.put(v, triple);
            tripleToVar.put(triple, v);
            return new Quad(v.asNode(), triple);
        }).toList();

        Op previous = new OpQuad(quads.get(0)); // at least one
        for (int i = 1; i < quads.size() ; ++i) {
            previous = OpJoin.create(previous, new OpQuad(quads.get(i)));
        }
        return previous;
    }

    private static Var getNewVar() {
        // small chance of collision
        return Var.alloc("g_" + (int) (Math.random() * Integer.MAX_VALUE));
    }
}
