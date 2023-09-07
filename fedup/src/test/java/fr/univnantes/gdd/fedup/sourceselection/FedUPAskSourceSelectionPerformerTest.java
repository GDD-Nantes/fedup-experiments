package fr.univnantes.gdd.fedup.sourceselection;

import com.bigdata.rdf.sail.webapp.QueryServlet;
import fr.univnantes.gdd.fedup.DummyDatasetA;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.rdf4j.common.webapp.util.HttpServerUtil;
import org.eclipse.rdf4j.http.client.RDF4JProtocolSession;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.http.config.HTTPRepositoryConfig;
import org.eclipse.rdf4j.repository.http.helpers.HTTPRepositorySettings;
import org.eclipse.rdf4j.spring.support.RDF4JTemplate;
import org.junit.jupiter.api.Test;
import org.openrdf.repository.http.config.HTTPRepositoryFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class FedUPAskSourceSelectionPerformerTest {

    @Test
    public void test_starting_simple_spring_server () throws IOException {

        DummyDatasetA datasetA = new DummyDatasetA();

        QueryServlet queryServlet = new QueryServlet();



        System.out.println("meow");


        // https://github.com/eclipse-rdf4j/rdf4j/blob/8903c85733ce12e948055792e681b869c7559ecb/tools/server/src/test/java/org/eclipse/rdf4j/http/server/TestServer.java


        // RDF4JTemplate template = new RDF4JTemplate();

    }

}