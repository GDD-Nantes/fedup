package fr.gdd.fedup.source.selection.transforms;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.TransformSingle;
import org.apache.jena.sparql.algebra.op.OpTable;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * Values inserted "placeholder" variable before graph variables ?g1 ?g2… where
 * assigned. We replace the placeholder with the proper variable name.
 */
public class ToValuesWithoutPlaceholderTransform extends TransformSingle {

    ToQuadsTransform toQuads;
    ToValuesTransform toValues;

    public ToValuesWithoutPlaceholderTransform(ToQuadsTransform toQuads, ToValuesTransform toValues) {
        this.toQuads = toQuads;
        this.toValues = toValues;
    }

    @Override
    public Op transform(OpTable opTable) {
        if (toValues.values2triple.containsKey(opTable)) {
            return newTableFromOldOne(toValues.values2triple.get(opTable), opTable);
        }
        return super.transform(opTable);
    }

    public OpTable newTableFromOldOne(Triple t, OpTable opTable) {
        Var v = toQuads.triple2Var.get(t);
        TableN table = new TableN();
        opTable.getTable().rows().forEachRemaining(b ->
                table.addBinding(Binding.builder().add(v, b.get("placeholder")).build())
                );
        return OpTable.create(table);
    }
}
