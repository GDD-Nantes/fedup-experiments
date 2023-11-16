package fr.univnantes.gdd.fedup.strategies;

import org.apache.jena.graph.Node;

public class Identity extends LeavePredicateUntouched {

    @Override
    Node transform(Node node) {
        return node;
    }
}
