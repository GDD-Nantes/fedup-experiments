package org.aksw.simba.quetsal.configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import com.fluidops.fedx.Config;

/**
 * Quetzal configurations setup. Need to run one time in the start before query execution
 * @author Saleem
 */
public class QuetzalConfig {
	static Logger log = LoggerFactory.getLogger(Config.class);

	public ArrayList<String> dataSources = new  ArrayList<String>() ;
	public ArrayList<String> commonPredicates = new ArrayList<String>(); // list of common predicates. Note we use this in ASK_dominent Source selection Algorithm
	public double commonPredThreshold; // threshold value for a predicate (in % of total data sources) to be considered in common predicate list
	
	public static enum Mode {INDEX_DOMINANT, ASK_DOMINANT}
	
	public Mode mode;

	private String prefixSummary;

	/**
	 * Quetzal Configurations. Must call this method once before starting source selection.
	 * mode can be either set to Index_dominant or ASK_dominant. See details in FedSum paper.
	 */
	public QuetzalConfig(Config config, String prefixSummary) {
		this.prefixSummary = prefixSummary;
		this.mode = Mode.valueOf(config.getProperty("quetzal.mode", "ASK_DOMINANT"));
		this.commonPredThreshold = Double.parseDouble(config.getProperty("quetzal.inputCommonPredThreshold", "0.33"));
		this.loadCommonPredList(config);
	}

	public QuetzalConfig(Config config) {
		this(config, "http://aksw.org/quetsal/");
	}

	public void loadCommonPredList(Config config) {
		File curfile = new File("summaries/memorystore.data");
		curfile.delete();

		File fileDir = new File("summaries");
		Repository repository = new SailRepository(new MemoryStore(fileDir));

		try {
			repository.initialize();
			RepositoryConnection connection = repository.getConnection();
			try {
				String summary = config.getProperty("quetzal.fedSummaries");
				connection.add(new File(summary), "aksw.org.simba", RDFFormat.N3);
				
				// retrieving data sources
				String query = "SELECT DISTINCT ?url WHERE {?s <" + this.prefixSummary + "url> ?url}";
				TupleQueryResult result = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
				ArrayList<String> dataSources = new ArrayList<>();
				while(result.hasNext()) {
					dataSources.add(result.next().getValue("url").stringValue());
				}
				result.close();

				// retrieving common predicates
				query = "SELECT DISTINCT ?p WHERE {?s <" + this.prefixSummary + "predicate> ?p}";
				result = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
				ArrayList<String> predicates = new ArrayList<>();
				while(result.hasNext()) {
					predicates.add(result.next().getValue("p").stringValue());
				}
				result.close();
				for(String predicate: predicates) {
					int count = 0;
					query = String.join(
                        " ",
                        "PREFIX ds: <" + this.prefixSummary + ">",
                        "SELECT DISTINCT ?url",
                        "WHERE { ?s ds:url ?url . ?s ds:capability ?cap . ?cap ds:predicate <" + predicate + ">}");
					result = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
					while(result.hasNext()) {
						result.next();
						count++;
					}
					result.close();
					double threshold = (double) count / dataSources.size();
					if(threshold >= commonPredThreshold) {
						commonPredicates.add(predicate); 
					}		
				}
			} catch (RDFParseException|RepositoryException|IOException e) {
				e.printStackTrace();
			} finally {
				connection.close();
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		} finally {
			repository.shutDown();
		}
	}
}
