package fr.gdd.fedup.startup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

import com.fluidops.fedx.Config;
import com.fluidops.fedx.FedXFactory;
import com.fluidops.fedx.Util;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.sail.FedXSailRepository;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.Utils;
import fr.univnantes.gdd.fedup.sourceselection.SourceAssignments;
import fr.univnantes.gdd.fedup.sourceselection.SourceSelectionPerformer;

@RestController
public class FedUPController {

	private static Logger logger = LogManager.getLogger(FedUPController.class);

	private List<String> loadEndpoints(String fileName) throws IOException {
        Path filePath = Path.of(fileName);
        String endpoints = Files.readString(filePath);
        return Arrays.asList(endpoints.split("\n"));
    }

	@PostMapping("/fedSparql")
	public Spy onFedQuery(
		@RequestBody InputParameters parameters
	) throws Exception {
		List<String> endpoints;
		try {
			endpoints = this.loadEndpoints(parameters.endpointsFileName);
		} catch (IOException exception) {
			logger.error("Error when loading endpoints: " + exception);
			return null;
		}

		Spy spy = new Spy();
		Config config = new Config(parameters.configFileName);

		FedXSailRepository repository = FedXFactory.initializeSparqlFederation(config, endpoints);
		SailRepositoryConnection connection = repository.getConnection();
            
		SourceSelectionPerformer sourceSelectionPerformer = (SourceSelectionPerformer) Util.instantiate(
			config.getProperty("fedup.sourceSelectionClass"), connection);

		List<Map<StatementPattern, List<StatementSource>>> assignments;
		assignments = sourceSelectionPerformer.performSourceSelection(parameters.queryString, parameters.assignments, spy);

		spy.tpwss = Utils.computeTPWSS(assignments);

		// logger.debug("Assignments: " + assignments);

		SourceAssignments sourceSelection = SourceAssignments.getInstance();
		Collections.shuffle(assignments);
		sourceSelection.setAssignments(assignments);
		
		if (parameters.runQuery) {
			FedUPQueryExecutor executor = new FedUPQueryExecutor(connection);
			executor.execute(parameters.queryString, sourceSelection, spy);
		}

		logger.info("Source Selection Time: " + spy.sourceSelectionTime + "ms");
		logger.info("Execution Time: " + spy.executionTime + "ms");

		connection.close();
		repository.shutDown();

		return spy;
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
