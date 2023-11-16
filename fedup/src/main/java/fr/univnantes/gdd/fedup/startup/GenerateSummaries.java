package fr.univnantes.gdd.fedup.startup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import fr.univnantes.gdd.fedup.summary.IdentitySummarizer;
import fr.univnantes.gdd.fedup.summary.Summarizer;
import org.aksw.simba.quetsal.util.HibiscusSummariesGenerator;
import org.aksw.simba.quetsal.util.TBSSSummariesGenerator;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.atlas.io.IndentedLineBuffer;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.base.Sys;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.log4j.Logger;

import fr.univnantes.gdd.fedup.summary.HashSummarizer;

public class GenerateSummaries {

    private static final Logger logger = Logger.getLogger(GenerateSummaries.class);

    private static List<String> loadEndpoints(String filename) throws IOException {
        Path filePath = Path.of(filename);
        String endpoints = Files.readString(filePath);
        return Arrays.asList(endpoints.split("\n"));
    }

//    private static String loadQuery(String filename) throws IOException {
//        Path filePath = Path.of(filename);
//        return Files.readString(filePath);
//    }
//
//    private static void writeQuery(String queryString, String filename) throws IOException {
//        Path filePath = Path.of(filename);
//        Files.writeString(filePath, queryString);
//    }
//
//    private static String rewriteQuery(String queryString, int modulo) throws Exception {
//        Query query = QueryFactory.create(queryString);
//        Query newQuery = new HashSummarizer(modulo).summarize(query);
//        logger.info("oldQuery: " + query.toString());
//        logger.info("newQuery: " + newQuery.toString());
//        return newQuery.toString();
//    }
//
//    private static void summarizeDataset(String filename, String output, int modulo) throws IOException {
//        File file = new File(output);
//        FileWriter fileWriter = new FileWriter(file);
//        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
//        IteratorCloseable<Quad> iter = AsyncParser.asyncParseQuads(filename);
//        HashSummarizer summarizer = new HashSummarizer(modulo);
//        NodeFormatterNT formatterNT = new NodeFormatterNT();
//        Set<Integer> seen = new TreeSet<>();
//        Integer[] num_quads = {0};
//        iter.forEachRemaining(quad -> {
//            try {
//                Quad newQuad = summarizer.summarize(quad);
//                int hashcode = newQuad.toString().hashCode();
//                if (!seen.contains(hashcode)) {
//                    IndentedWriter writer = new IndentedLineBuffer();
//                    formatterNT.format(writer, newQuad.getSubject());
//                    writer.print(" ");
//                    formatterNT.format(writer, newQuad.getPredicate());
//                    writer.print(" ");
//                    formatterNT.format(writer, newQuad.getObject());
//                    writer.print(" ");
//                    formatterNT.format(writer, newQuad.getGraph());
//                    writer.println(".");
//                    bufferedWriter.write(writer.toString());
//                    writer.close();
//                    seen.add(hashcode);
//                }
//                num_quads[0] += 1;
//                if (num_quads[0] % 1000000 == 0) {
//                    System.out.println("Num Quads: " + num_quads[0]);
//                }
//            } catch (IOException e) {
//                logger.warn(quad + " has been rejected because an error occured: " + e);
//            }
//        });
//        bufferedWriter.flush();
//        bufferedWriter.close();
//        fileWriter.close();
//    }

    private static void hibiscus(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("e", "endpoints", true, "list of endpoints");
        options.addOption("o", "output", true, "summary output file");
        
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        List<String> endpoints;
        if (cmd.hasOption("endpoints")) {
            endpoints = loadEndpoints(cmd.getOptionValue("endpoints"));
        } else {
            throw new Exception("No endpoints provided");
        }

        String output;
        if (cmd.hasOption("output")) {
            output = cmd.getOptionValue("output");
        } else {
            throw new Exception("No output file specified");
        }

        HibiscusSummariesGenerator generator = new HibiscusSummariesGenerator(output);
        generator.generateSummaries(endpoints, null);
    }

    private static void costfed(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("e", "endpoints", true, "list of endpoints");
        options.addOption("b", "branching-factor", true, "costfed summaries parameter");
        options.addOption("o", "output", true, "summary output file");
        
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        List<String> endpoints;
        if (cmd.hasOption("endpoints")) {
            endpoints = loadEndpoints(cmd.getOptionValue("endpoints"));
        } else {
            throw new Exception("No endpoints provided");
        }

        int branchLimit;
        if (cmd.hasOption("branching-factor")) {
            branchLimit = Integer.parseInt(cmd.getOptionValue("branching-factor"));
        } else {
            branchLimit = 4; // used in the CostFed paper
        }

        String output;
        if (cmd.hasOption("output")) {
            output = cmd.getOptionValue("output");
        } else {
            throw new Exception("No output file specified");
        }

        TBSSSummariesGenerator generator = new TBSSSummariesGenerator(output);
        generator.generateSummaries(endpoints, null, branchLimit);
    }

    private static void fedup(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("e", "endpoints", true, "list of endpoints");
        options.addOption("m", "modulo", true, "compactness of the summary");
        options.addOption("t", "threads", true, "number of threads");
        options.addOption("o", "output", true, "summary output file");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        List<String> endpoints;
        if (cmd.hasOption("endpoints")) {
            endpoints = loadEndpoints(cmd.getOptionValue("endpoints"));
        } else {
            throw new Exception("No endpoints provided");
        }

        int modulo;
        if (cmd.hasOption("modulo")) {
            modulo = Integer.parseInt(cmd.getOptionValue("modulo"));
        } else {
            modulo = 0;
        }

        int nbThreads;
        if (cmd.hasOption("threads")) {
            nbThreads = Integer.parseInt(cmd.getOptionValue("threads"));
        } else {
            nbThreads = 8;
        }

        String output;
        if (cmd.hasOption("output")) {
            output = cmd.getOptionValue("output");
        } else {
            throw new Exception("No output file specified");
        }

        // #1 - Getting the number of triples in each graph

        System.out.println("Computing cardinalities...");

        List<Future<ImmutablePair<String, Integer>>> futures = new ArrayList<>();
        Map<String, Integer> cardinalities = new HashMap<>();

        try (ExecutorService executor = Executors.newFixedThreadPool(nbThreads)) {
            for (String endpoint : endpoints) {
                Future<ImmutablePair<String, Integer>> future = executor.submit(() -> {
                    String queryString = String.format("""
                            SELECT * WHERE {
                                SERVICE <%s> { SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o } }
                            }""", endpoint);
                    Query query = QueryFactory.create(queryString);

                    Dataset dataset = DatasetFactory.create();
                    dataset.begin(ReadWrite.READ);

                    int cardinality = 0;

                    QueryIterator iterator = Algebra.exec(Algebra.compile(query), dataset);
                    if (iterator.hasNext()) {
                        Binding binding = iterator.next();
                        cardinality = Integer.parseInt(binding.get("count").getLiteralValue().toString());
                    }

                    dataset.commit();
                    dataset.end();

                    return new ImmutablePair<>(endpoint, cardinality);
                });
                futures.add(future);
            }
        }

        for (Future<ImmutablePair<String, Integer>> future : futures) {
            ImmutablePair<String, Integer> result = future.get();
            cardinalities.put(result.getLeft(), result.getRight());
        }

        // #2 - Paginating queries because Virtuoso cannot return more than 1M results

        System.out.println("Paginating queries...");

        List<String> paginatedQueries = new ArrayList<>();

        for (String endpoint : cardinalities.keySet()) {
            int limit = 1000000;
            int offset = 0;
            while (offset < cardinalities.get(endpoint)) {
                String queryString = String.format("""
                        SELECT * WHERE {
                            BIND(<%s> AS ?g)
                            SERVICE <%s> { SELECT * WHERE { ?s ?p ?o } OFFSET %d LIMIT %d }
                        }""", endpoint, endpoint, offset, limit);
                paginatedQueries.add(queryString);
                offset += limit;
            }
        }
        
        // #3 - Running queries in parallel

        System.out.println("Summarizing endpoints...");

        List<Future<Set<Quad>>> quadCollectors = new ArrayList<>();

        try (ExecutorService executor = Executors.newFixedThreadPool(nbThreads)) {
            for (String paginatedQuery : paginatedQueries) {
                Future<Set<Quad>> future = executor.submit(() -> {
                    Summarizer summarizer;
                    if (modulo < 0) {
                        summarizer = new IdentitySummarizer();
                    } else {
                        summarizer = new HashSummarizer(modulo);
                    }

                    System.out.println(paginatedQuery);

                    Query query = QueryFactory.create(paginatedQuery);
                    Dataset dataset = DatasetFactory.create();
                    dataset.begin(ReadWrite.READ);

                    QueryIterator iterator = Algebra.exec(Algebra.compile(query), dataset);

                    Set<Quad> quads = new HashSet<>();

                    while (iterator.hasNext()) {
                        Binding binding = iterator.next();
                        Triple triple = Triple.create(binding.get("s"), binding.get("p"), binding.get("o"));
                        Node graph = NodeFactory.createURI(binding.get("g").getURI().split("default-graph-uri=")[1]);
                        Quad quad = Quad.create(binding.get("g"), triple);
                        Quad summarizedQuad = summarizer.summarize(quad);
                        quads.add(summarizedQuad);
                    }

                    dataset.commit();
                    dataset.end();

                    return quads;
                });
                quadCollectors.add(future);
            }
        }

        // #4 - Merging quads

        System.out.println("Merging quads...");

        Set<Quad> quads = new HashSet<>();
        for (Future<Set<Quad>> quadCollector : quadCollectors) {
            quads.addAll(quadCollector.get());
        }

        // #5 - Writing the .nq file

        System.out.println("Writing quads...");

        File file = new File(output);
        FileWriter fileWriter = new FileWriter(file);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        NodeFormatterNT formatterNT = new NodeFormatterNT();

        for (Quad quad : quads) {
            IndentedWriter writer = new IndentedLineBuffer();
            formatterNT.format(writer, quad.getSubject());
            writer.print(" ");
            formatterNT.format(writer, quad.getPredicate());
            writer.print(" ");
            formatterNT.format(writer, quad.getObject());
            writer.print(" ");
            formatterNT.format(writer, quad.getGraph());
            writer.println(".");
            bufferedWriter.write(writer.toString());
            writer.close();
        }

        bufferedWriter.flush();
        bufferedWriter.close();
        fileWriter.close();
    } 
    
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new Exception("No command specified");
        } else if (args[0].equals("hibiscus")) {
            hibiscus(args);
        } else if (args[0].equals("costfed")) {
            costfed(args);
        } else if (args[0].equals("fedup")) {
            fedup(args);
        } else {
            throw new Exception("Unknown command '" + args[0] + "'");
        }
    }
}
