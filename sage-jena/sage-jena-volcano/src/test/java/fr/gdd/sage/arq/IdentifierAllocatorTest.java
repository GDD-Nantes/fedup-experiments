package fr.gdd.sage.arq;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.sse.SSE;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IdentifierAllocatorTest {

    private static Logger log = LoggerFactory.getLogger(IdentifierAllocatorTest.class);

    @Test
    public void simple_test_with_only_one_bgp () {
        Op op = SSE.parseOp("(bgp (?s <http://www.geonames.org/ontology#parentCountry> ?o))");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        op.visit(new OpVisitorBase() {
            @Override
            public void visit(OpBGP opBGP) {
                assertEquals(List.of(1), a.getIds(opBGP));
            }
        });
    }

    @Test
    public void two_bgps_linked_by_a_join () {
        Op op = SSE.parseOp("(join " +
                "(bgp (?s <http://A> ?o)(?s <http://C> ?o)) " +
                "(bgp (?s <http://B> ?o)))");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        op.visit(new OpVisitorBase() {
            @Override
            public void visit(OpBGP opBGP) {
                switch (opBGP.getPattern().get(0).getPredicate().toString()) {
                    case "http://A" -> assertEquals(List.of(1, 2), a.getIds(opBGP));
                    case "http://B" -> assertEquals(List.of(3), a.getIds(opBGP));
                    default -> assertFalse(true);
                }
            }

            @Override
            public void visit(OpJoin opJoin) {
                opJoin.getLeft().visit(this);
                opJoin.getRight().visit(this);
            }
        });
    }

    @Test
    public void identical_operator_built_independently_return_the_id () {
        Op op = SSE.parseOp("(join " +
                "(bgp (?s <http://A> ?o)(?s <http://C> ?o)) " +
                "(bgp (?s <http://B> ?o)))");
        IdentifierAllocator a = new IdentifierAllocator();
        op.visit(a);

        Op op1 = SSE.parseOp("(bgp (?s <http://A> ?o)(?s <http://C> ?o))");
        assertEquals(List.of(1, 2), a.getIds(op1));
        Op op2 = SSE.parseOp("(bgp (?s <http://B> ?o))");
        assertEquals(List.of(3), a.getIds(op2));
    }

}