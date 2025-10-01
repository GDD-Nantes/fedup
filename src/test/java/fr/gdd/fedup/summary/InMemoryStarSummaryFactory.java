package fr.gdd.fedup.summary;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Different way to insert sources into the dataset using
 * RDF-star.
 */
public class InMemoryStarSummaryFactory {

    Dataset petsDataset;
    // TODO the corresponding summary

    public Dataset getPetsDataset() {
        if (Objects.nonNull(petsDataset)) {
            return petsDataset;
        }

        petsDataset = TDB2Factory.createDataset();
        petsDataset.begin(ReadWrite.WRITE);

        List<String> statements = Arrays.asList(
                "<<<http://auth/person> <http://auth/named> <http://auth/Alice>>>  <http://example.org/occurrenceOf> <http://graphA.org> .",
                "<<<http://auth/person> <http://auth/named> <http://auth/Bob>>>    <http://example.org/occurrenceOf> <http://graphA.org> .",
                "<<<http://auth/Alice>  <http://auth/owns>  <http://auth/cat>>>    <http://example.org/occurrenceOf> <http://graphA.org> .",

                "<<<http://auth/person> <http://auth/named> <http://auth/Carol>>>  <http://example.org/occurrenceOf> <http://graphB.org> .",
                "<<<http://auth/person> <http://auth/named> <http://auth/David>>>  <http://example.org/occurrenceOf> <http://graphB.org> .",
                "<<<http://auth/Bob>  <http://auth/owns>  <http://auth/dog>>>    <http://example.org/occurrenceOf> <http://graphB.org> .",
                "<<<http://auth/David>  <http://auth/nbPets> \"2\">>               <http://example.org/occurrenceOf> <http://graphB.org> .",
                "<<<http://auth/dog>    <http://auth/family> <http://auth/canid>>> <http://example.org/occurrenceOf> <http://graphB.org> ."
        );

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());
        petsDataset.setDefaultModel(model);

        petsDataset.commit();
        petsDataset.end();

        return petsDataset;
    }
}
