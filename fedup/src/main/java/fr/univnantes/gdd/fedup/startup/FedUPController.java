package fr.univnantes.gdd.fedup.startup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.fluidops.fedx.*;
import fr.univnantes.gdd.fedup.sourceselection.UoJvsJoU;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.sail.FedXSailRepository;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.Utils;
import fr.univnantes.gdd.fedup.sourceselection.SourceSelectionPerformer;

@RestController
public class FedUPController {

	private static final Logger logger = LogManager.getLogger(FedUPController.class);

	private List<String> loadEndpoints(String fileName) throws IOException {
        Path filePath = Path.of(fileName);
        String endpoints = Files.readString(filePath);
        return Arrays.asList(endpoints.split("\n"));
    }

	@PostMapping("/fedSparql")
		public Spy onFedQuery(
		@RequestBody InputParameters parameters
	) throws Exception {
		Spy.reset(); // to avoid cumulating metrics of previous runs

		long startTime = System.currentTimeMillis();

		Config config = new Config(parameters.configFileName);

		FedXSailRepository repository = FedXFactory.initializeSparqlFederation(config, parameters.endpoints);
		SailRepositoryConnection connection = repository.getConnection();
            
		SourceSelectionPerformer sourceSelectionPerformer = (SourceSelectionPerformer) Util.instantiate(
			config.getProperty("fedup.sourceSelectionClass"), connection);

		List<Map<StatementPattern, List<StatementSource>>> assignments;
		assignments = sourceSelectionPerformer.performSourceSelection(parameters.queryString);

		// new PrintQueryPlans((FedXConnection) connection.getSailConnection()).print(parameters.queryString, assignments);

		Spy.getInstance().tpwss = Utils.computeTPWSS(assignments);

		// logger.debug("Assignments: " + assignments);

		// global assign so FedX can pick its source assignments one by one afterward
		// SourceAssignments sourceSelection = SourceAssignments.getInstance();
		// Collections.shuffle(assignments);
		// sourceSelection.setAssignments(assignments);

		// (TODO) RDF4J does not support values
		// (TODO) ugly testing by quickly removing the values
		// String queryString = parameters.queryString.replaceAll("(VALUES|values).*", "");
		String queryString = parameters.queryString;
		System.out.println(queryString);
		if (parameters.runQuery) {
			FedUPQueryExecutor executor = new FedUPQueryExecutor(connection);
			executor.execute(queryString, assignments);
		}

		logger.info("Source Selection Time: " + Spy.getInstance().sourceSelectionTime + "ms");
		logger.info("Execution Time: " + Spy.getInstance().executionTime + "ms");

		connection.close();
		repository.shutDown();

		long endTime = System.currentTimeMillis();

		Spy.getInstance().runtime = endTime - startTime;

		return Spy.getInstance();
	}

	@GetMapping("/sparql")
	public Spy onQuery(
		@RequestParam(name = "query") String queryString,
		@RequestParam(name = "dataset") String datasetLocation
	) throws Exception {
		Dataset dataset = TDB2Factory.connectDataset(datasetLocation);
        dataset.begin();

		QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineFactory factory = QueryEngineSage.factory;
		QueryEngineRegistry.addFactory(factory);

		Context context = dataset.getContext().copy();
		context.set(SageConstants.timeout, Long.MAX_VALUE);
        
        Query query = QueryFactory.create(queryString);
        Plan plan = factory.create(query, dataset.asDatasetGraph(), BindingRoot.create(), context);
        QueryIterator iterator = plan.iterator();

		Spy spy = new Spy();
		System.out.println(query);
		System.out.println(datasetLocation);
		System.out.println(iterator.hasNext());
		while (iterator.hasNext()) {
            System.out.println(iterator.next());
			spy.numSolutions += 1;
		}

		dataset.end();

		return spy;
	}
}
