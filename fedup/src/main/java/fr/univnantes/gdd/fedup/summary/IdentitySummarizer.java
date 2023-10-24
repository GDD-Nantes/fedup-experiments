package fr.univnantes.gdd.fedup.summary;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Quad;

public class IdentitySummarizer extends Summarizer {

    public IdentitySummarizer(int... args) {
        super(args);
    }

    @Override
    public Node summarize(Node node) {
        return node;    
    }

    @Override
    public Triple summarize(Triple triple) {
        return triple;
    }

    @Override
    public Quad summarize(Quad quad) {
        return quad;
    }

    @Override
    public Query summarize(Query query) {
        return query;
    }

    @Override
    public Op summarize(Op query) {
        return query;
    }
    
}
