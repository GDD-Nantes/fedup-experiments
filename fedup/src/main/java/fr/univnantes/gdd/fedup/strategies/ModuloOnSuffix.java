package fr.univnantes.gdd.fedup.strategies;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.core.Var;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Good old hashing on URI suffix.
 */
public class ModuloOnSuffix extends LeavePredicateUntouched {

    Integer modulo = 1;

    public ModuloOnSuffix(Integer modulo) {
        this.modulo = modulo;
    }

    public Node transform(Node node) {
        if (node.isURI()) {
            try {
                URI uri = new URI(node.getURI());
                int hashcode = Math.abs(uri.toString().hashCode());
                if (modulo == 0 || modulo == 1) {
                    return NodeFactory.createURI(uri.getScheme() + "://" + uri.getHost());
                } else {
                    return NodeFactory.createURI(uri.getScheme() + "://" + uri.getHost() + "/" + (hashcode % modulo));
                }
            } catch (URISyntaxException e) {
                return NodeFactory.createURI("https://donotcare.com/whatever");
            }
        } else if (node.isLiteral()) {
            return NodeFactory.createLiteral("any");
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
