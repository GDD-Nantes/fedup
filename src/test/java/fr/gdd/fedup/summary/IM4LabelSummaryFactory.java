package fr.gdd.fedup.summary;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A simple in memory summary to help with testing.
 */
public class IM4LabelSummaryFactory implements AutoCloseable {

    Dataset peopleDataset;
    Summary simplePeopleSummary;

    /**
     * @return A dataset comprising triples about pets and ownership distributed
     *         in two graphs.
     */
    public Dataset getPeopleDataset() {
        if (Objects.isNull(peopleDataset)) {
            getSimplePeopleSummary();
        }
        return peopleDataset;
    }

    /**
     * @return The summary dataset of the dataset about pets.
     */
    public Summary getSimplePeopleSummary() {
        if (Objects.nonNull(simplePeopleSummary)) {
            return simplePeopleSummary;
        }
        Summary summary = SummaryFactory.createModuloOnSuffix(1);
        Dataset dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        List<String> statements = Arrays.asList(
                "<http://graphA/Alice> <http://auth/type> <http://auth/person>. ",
                "<http://graphA/Alice> <http://auth/label> \"Alice\". "
        );

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model modelA = ModelFactory.createDefaultModel();
        modelA.read(statementsStream, "", Lang.NT.getLabel());

        statements = Arrays.asList(
                "<http://graphB/Bob> <http://auth/type> <http://auth/person>. ",
                "<http://graphB/Bob> <http://auth/label> \"Bob\". "
        );
        statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model modelB = ModelFactory.createDefaultModel();
        modelB.read(statementsStream, "", Lang.NT.getLabel());

        dataset.addNamedModel("https://graphA.org", modelA);
        dataset.addNamedModel("https://graphB.org", modelB);

        dataset.commit();
        dataset.end();
        dataset.begin(ReadWrite.READ);
        Iterator<Quad> quads = dataset.asDatasetGraph().find();

        while (quads.hasNext()) {
            summary.add(quads.next());
        }

        dataset.commit();
        dataset.end();

        peopleDataset = dataset;
        simplePeopleSummary = summary;

        return simplePeopleSummary;
    }

    public Dataset getGraph(String name) {
        getSimplePeopleSummary();
        Model modelA = peopleDataset.getNamedModel(name);
        Dataset dataset = TDB2Factory.createDataset();
        peopleDataset.begin(ReadWrite.READ);
        dataset.begin(ReadWrite.WRITE);
        dataset.setDefaultModel(modelA);
        dataset.commit();
        dataset.close();
        peopleDataset.commit();
        peopleDataset.close();
        return dataset;
    }

    @Override
    public void close() {
        if (Objects.nonNull(peopleDataset)) {
            if (peopleDataset.isInTransaction()) peopleDataset.commit();
            peopleDataset.close();
            TDBInternal.expel(peopleDataset.asDatasetGraph());
            peopleDataset = null;
        }
        if (Objects.nonNull(simplePeopleSummary)) {
            if (simplePeopleSummary.getSummary().isInTransaction()) simplePeopleSummary.getSummary().commit();
            simplePeopleSummary.getSummary().close();
            TDBInternal.expel(simplePeopleSummary.getSummary().asDatasetGraph());
            simplePeopleSummary = null;
        }
    }
}
