package fr.univnantes.gdd.fedup.strategies;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.core.Var;

import javax.print.attribute.URISyntax;
import java.net.URI;

public class ModuloOnWhole extends LeavePredicateUntouched {

    Integer modulo = 1;

    public ModuloOnWhole(Integer modulo) {
        this.modulo = modulo;
    }

    public Node transform(Node node) {
        if (node.isURI()) {
            int hashcode = Math.abs(node.getURI().hashCode());
            return NodeFactory.createURI("https://"+ (hashcode % modulo));
        } else if (node.isLiteral()) {
            // could do a test depending on type of the literal
            int hashcode = Math.abs(node.getLiteral().toString().hashCode());
            return NodeFactory.createLiteral(String.valueOf(hashcode));
        } else {
            return Var.alloc(node.getName());
        }
    }

    /* ************************************************************************* */

    @Override
    public Op transform(OpFilter opFilter, Op subOp) {
        return Transformer.transform(this, subOp); // TODO: handle special filter expressions, i.e., we don't want to remove simple equalities
    }

}
