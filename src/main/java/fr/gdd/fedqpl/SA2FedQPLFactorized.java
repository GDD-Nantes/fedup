package fr.gdd.fedqpl;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.OpCloningUtil;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import fr.gdd.fedup.transforms.ToQuadsTransform;
import org.apache.commons.collections4.MultiSet;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.table.Table1;
import org.apache.jena.sparql.algebra.table.TableN;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingProject;

import java.util.*;

/**
 * A source assignments is a list of sources that are expected to provide
 * actual results. By itself, it is not sufficient to provide correct and
 * complete results of the query.
 * *
 * Along with the original query plan, this visitor converts the source assignments
 * into a FedQPL expression that encodes the federated query to perform.
 */
public class SA2FedQPLFactorized extends SA2FedQPL {

    public static Op build(Op query, ToQuadsTransform tqt, SAAsKG assignmentsAsKG) {
        SA2FedQPLFactorized builder = new SA2FedQPLFactorized(tqt, assignmentsAsKG);
        List<Op> subExps = ReturningOpVisitorRouter.visit(builder, query);
        Mu rootUnion = new Mu(subExps.stream().toList());

        if (Objects.isNull(builder.topMostProjection)) {
            return OpCloningUtil.clone(SA2FedQPL.createOpProjectWithAllVariables(query), rootUnion);
        } else {
            return rootUnion;
        }
    }

    /* *************************************************************** */

    public static boolean SILENT = true;

    public SA2FedQPLFactorized(ToQuadsTransform tqt, SAAsKG assignmentsAsKG) {
        super(tqt, assignmentsAsKG);
    }

    @Override
    public List<Op> visit(OpBGP opBGP) {
        // Could do all possibilities by calling all sub-triple pattern
        // then examine which combinations actually checks out. But this would
        // be very inefficient.
        // Instead, checking directly which results exist
        Set<Var> graphs = toQuads.findVars(opBGP);
        MultiSet<Binding> bindings = this.sols(opBGP);

        if (bindings.isEmpty()) { // no sources were found
            return new ArrayList<>();
        }

        Table factorized = new TableN();
        // bindings.forEach(factorized::addBinding);
        bindings.forEach(binding -> factorized.addBinding(new BindingProject(graphs, binding)));

        OpSequence sequenceOfServices = OpSequence.create();
        sequenceOfServices.add(OpTable.create(factorized));

        for (Var g : graphs) { // multi-union is replaced by a values
            OpTriple triple = new OpTriple(toQuads.getVar2quad().get(g).asTriple());
            Op req = new OpService(g, triple, SILENT);
            sequenceOfServices.add(req);
            toQuads.add(g, triple);
        }

        return List.of(sequenceOfServices);
    }

    @Override
    public List<Op> visit(OpJoin join) {
        List<Op> results = new ArrayList<>();

        // we want to examine each possibility once
        List<Op> lefts = new HashSet<>(ReturningOpVisitorRouter.visit(this, join.getLeft())).stream().toList();
        List<Op> rights = new HashSet<>(ReturningOpVisitorRouter.visit(this, join.getRight())).stream().toList();

        // still need to check one by one, not as multi-unions, (hence hashmap)
        lefts = new HashSet<>(fromValuesToListOfOps(lefts.getFirst())).stream().toList();
        rights = new HashSet<>(fromValuesToListOfOps(rights.getFirst())).stream().toList();

        HashMap<Op, List<Op>> left2rights = new HashMap<>();
        for (Op left : lefts) { // for each mandatory part
            for (Op right : rights) { // examine the optional part
                if (this.ask(OpJoin.create(left, right))) {
                    left2rights.computeIfAbsent(left, k -> new ArrayList<>() ).add(right);
                }
            }
        }

        // factorize then
        HashMap<Op, List<Op>> rights2left = new HashMap<>();
        for (Map.Entry<Op, List<Op>> entry : left2rights.entrySet()) {
            Op leftFactorized = fromListOfOpsToOp(entry.getValue());
            rights2left.computeIfAbsent(leftFactorized, k -> new ArrayList<>()).add(entry.getKey());
        }

        List<Op> result = new ArrayList<>();

        for (Map.Entry<Op, List<Op>> entry : rights2left.entrySet()) {
            switch (entry.getValue().size()) { // simplify when possible
                case 0 -> throw new IllegalStateException("Should not be empty");
                case 1 -> result.add(OpCloningUtil.clone(join, entry.getValue().getFirst(), entry.getKey())); // only 1 child
                default -> result.add(OpCloningUtil.clone(join, fromListOfOpsToOp(entry.getValue()), entry.getKey()));
            }
        }

        return result;
    }

    @Override
    public List<Op> visit(OpLeftJoin lj) {
        // we want to examine each possibility once (hence hashmap)
        List<Op> lefts = new HashSet<>(ReturningOpVisitorRouter.visit(this, lj.getLeft())).stream().toList();
        List<Op> rights = new HashSet<>(ReturningOpVisitorRouter.visit(this, lj.getRight())).stream().toList();

        // still need to check one by one, not as multi-unions, (hence hashmap)
        lefts = new HashSet<>(fromValuesToListOfOps(lefts.getFirst())).stream().toList();
        rights = new HashSet<>(fromValuesToListOfOps(rights.getFirst())).stream().toList();

        HashMap<Op, List<Op>> left2rights = new HashMap<>();
        for (Op left : lefts) { // for each mandatory part
            for (Op right : rights) { // examine the optional part
                if (this.ask(OpJoin.create(left, right))) {
                    left2rights.computeIfAbsent(left, k -> new ArrayList<>() ).add(right);
                } else {
                    left2rights.computeIfAbsent(left, k -> new ArrayList<>() );
                }
            }
        }

        // factorize then
        HashMap<Op, List<Op>> rights2left = new HashMap<>();
        Set<Op> alones = new HashSet<>();
        for (Map.Entry<Op, List<Op>> entry : left2rights.entrySet()) {
            if (entry.getValue().isEmpty()) {
                alones.add(entry.getKey()); // left without rights
            }
            Op leftFactorized = fromListOfOpsToOp(entry.getValue());
            rights2left.computeIfAbsent(leftFactorized, k -> new ArrayList<>()).add(entry.getKey());
        }

        List<Op> result = new ArrayList<>();
        if (!alones.isEmpty()) {
            result.add(fromListOfOpsToOp(alones.stream().toList())); // factorized
        }

        for (Map.Entry<Op, List<Op>> entry : rights2left.entrySet()) {
            switch (entry.getValue().size()) { // simplify when possible
                case 0 -> throw new IllegalStateException("Should not be empty");
                case 1 -> result.add(OpCloningUtil.clone(lj, entry.getValue().getFirst(), entry.getKey())); // only 1 child
                default -> result.add(OpCloningUtil.clone(lj, fromListOfOpsToOp(entry.getValue()), entry.getKey()));
            }
        }

        return result;
    }


/* ************************************************************************* */

    public List<Op> fromValuesToListOfOps(Op op) {
        return switch (op) {
            case OpSequence seq -> {
                switch (seq.get(0)) {
                    case OpTable table -> {
                        List<Op> results = new ArrayList<>();
                        table.getTable().rows().forEachRemaining(b-> {
                            OpSequence newSeq = OpSequence.create();
                            newSeq.add(OpTable.create(new Table1(b)));
                            for (int i = 1; i < seq.size(); i++) {
                                newSeq.add(seq.get(i));
                            }
                            results.add(newSeq);
                        });
                        yield results;
                    }
                    default -> throw new IllegalStateException("Unexpected value: " + seq);
                }
            }
            default -> List.of(op); //throw new IllegalStateException("Unexpected value: " + op);
        };
    }

    public Op fromListOfOpsToOp(List<Op> ops) {
        List<Op> result = new ArrayList<Op>();
        List<OpTable> tables = new ArrayList<>();
        ops.forEach(
            op -> {
                if (op instanceof OpSequence seq) {
                    tables.add((OpTable) seq.get(0));
                } else {
                    result.add(op);
                    // throw new IllegalStateException("Unexpected value: " + op);
                }
            });
        if (!result.isEmpty()) { return result.getFirst(); }

        Table newTable = new TableN();
        tables.forEach(table -> table.getTable().rows().forEachRemaining(newTable::addBinding));

        OpSequence newSeq = OpSequence.create();
        newSeq.add(OpTable.create(newTable));
        for (int i = 1; i < ((OpSequence) ops.get(0)).getElements().size(); i++) {
            Op op = ((OpSequence) ops.get(0)).getElements().get(i);
            newSeq.add(op); // copy the rest
        }

        return newSeq;
    }

}