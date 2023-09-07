package fr.univnantes.gdd.fedup;

import org.apache.jena.fuseki.main.FusekiServer;

/**
 * Run FedUP, a FEDerated Unions-over-joins plan-based SPARQL query engine.
 */
public class FedUP {

    public static void main(String[] args) {
        buildAndStart();
    }

    public static void buildAndStart() {
        FusekiServer.Builder serverBuilder = FusekiServer.create()
                // .parseConfigFile("configurations/sage.ttl")
                .enablePing(true)
                .enableCompact(true)
                .enableCors(true)
                .enableStats(true)
                .enableTasks(true)
                .enableMetrics(true)
                .port(8080)
                .numServerThreads(1, 10);

        serverBuilder.build().start();
    }

}
