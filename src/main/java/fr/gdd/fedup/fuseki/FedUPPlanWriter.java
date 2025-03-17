package fr.gdd.fedup.fuseki;

import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.Quad2Pattern;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.io.JSWriter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.util.Context;

import java.util.Objects;

public  class FedUPPlanWriter {
    public static void write(IndentedWriter writer, Context context) {

        Op fedQPLPlan = context.get(FedUPConstants.EXPORTED);
        Boolean shouldExport = context.get(FedUPConstants.EXPORT_PLANS);

        if (Objects.isNull(shouldExport) || !shouldExport || Objects.isNull(fedQPLPlan)) {
            return; // nothing to see here, move along
        }

        writer.print(" ,");
        writer.print(JSWriter.outputQuotedString(FedUPConstants.EXPORTED.getSymbol()));
        writer.println(" : ");

        // byte[] serialized = SerializationUtils.serialize();
        fedQPLPlan = ReturningOpVisitorRouter.visit(new Quad2Pattern(), fedQPLPlan);
        String encoded = OpAsQuery.asQuery(fedQPLPlan).toString();// Base64.encode(OpAsQuery.asQuery(fedQPLPlan).toString().getBytes());
        writer.println(JSWriter.outputQuotedString(encoded));
    }
}
