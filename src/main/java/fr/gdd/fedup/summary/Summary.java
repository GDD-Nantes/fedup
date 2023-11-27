package fr.gdd.fedup.summary;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transform;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpQuad;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;

import java.util.Objects;

public class Summary {

    Dataset summary;
    Transform strategy;

    public Summary(Transform strategy, Location... location) {
        this.strategy = strategy;
        if (Objects.nonNull(location) && location.length > 0) {
            summary = TDB2Factory.connectDataset(location[0]);
        } else {
            summary = TDB2Factory.createDataset();
        }
    }

    public void add(Quad quad) {
        summary.begin(TxnType.WRITE);
        OpQuad toAdd = (OpQuad) strategy.transform(new OpQuad(quad));
        Model model = summary.getNamedModel(toAdd.getQuad().getGraph().getURI());
        model.add(model.asStatement(toAdd.getQuad().asTriple()));
        summary.commit();
        summary.end();
    }

    public Dataset getSummary() {
        return summary;
    }

    public Transform getStrategy() { return strategy; }

    public Op transform(Op query) {
        return Transformer.transform(strategy, query);
    }

}
