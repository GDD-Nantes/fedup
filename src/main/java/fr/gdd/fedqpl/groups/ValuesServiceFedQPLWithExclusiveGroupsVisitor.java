package fr.gdd.fedqpl.groups;

import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.ReturningArgsOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Table;
import org.apache.jena.sparql.algebra.TableFactory;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;

import java.util.*;
import java.util.function.Supplier;

/**
 * Create exclusive groups when they are close from each other. It pushes
 * operators inside SERVICE clauses, so endpoints execute them themselves.
 * Execution and data are close from each other.
 *
 */
public class ValuesServiceFedQPLWithExclusiveGroupsVisitor extends ReturningOpBaseVisitor {

    public static boolean SILENT = true;

    private FedQPLWithExclusiveGroupsVisitor exclusiveGroupsVisitor = new FedQPLWithExclusiveGroupsVisitor();
    private Integer counter = 0;

    @Override
    public Op visit(OpUnion opUnion) {

        return OpUnion.create(
                ReturningOpVisitorRouter.visit(this, opUnion.getLeft()),
                ReturningOpVisitorRouter.visit(this, opUnion.getRight())
        );
    }

    @Override
    public Op visit(OpSequence op) {
        try {
            return applyExclusiveGroups(op);
        } catch (Exception e) {
            return super.visit(op);
        }
    }

    private Op applyExclusiveGroups(OpSequence op) {
        if(!(op instanceof OpSequence seq)) {
            throw new UnsupportedOperationException("Can't optimize op that is not a sequence");
        }
        if(seq.size() > 2){
            throw new UnsupportedOperationException("Can't optimize sequence that has more than 2 elements");
        }
        if (!(seq.getElements().getFirst() instanceof OpTable table)){
            throw new UnsupportedOperationException("Can't optimize sequence whose first op is not a table");
        }

        Op query = seq.getElements().get(1);

        Map<Op, List<Binding>> decompositions2values = new HashMap<>();


        Integer max = 0;
        Iterator<Binding> rows = table.getTable().rows();
        while(rows.hasNext()) {
            Binding binding = rows.next();

            Op instantiated = Substitute.inject(query, binding);
            instantiated = ReturningOpVisitorRouter.visit(new RemoveAssignsAndExtends(),instantiated);

            Op grouped = ReturningOpVisitorRouter.visit(exclusiveGroupsVisitor, instantiated);

            DecompositionVisitor decompositionVisitor = new DecompositionVisitor();
            Integer counterBefore = counter;
            Op decomposed = ReturningOpVisitorRouter.visit(decompositionVisitor, grouped);
            max = Math.max(max, counter);
            counter = counterBefore;

            if(!decompositions2values.containsKey(decomposed)) {
                decompositions2values.put(decomposed, new ArrayList<>());
            }

            BindingBuilder builder = BindingBuilder.create();

            Map<OpService, Node> group2source = decompositionVisitor.group2source;
            for(Map.Entry<OpService, Node> serviceToSource : group2source.entrySet()) {
                builder.add((Var) serviceToSource.getKey().getService(), serviceToSource.getValue());
            }

            decompositions2values.get(decomposed).add(builder.build());
        }

        counter = max;

        List<Op> roots = new ArrayList<>();

        for (Op decomposition : decompositions2values.keySet()) {
            Table decompositionValues = TableFactory.create();
            for (Binding binding : decompositions2values.get(decomposition)) {
                decompositionValues.addBinding(binding);
            }

            Op sequence = OpSequence.create(OpTable.create(decompositionValues), decomposition);

            roots.add(sequence);
        }

        return roots.size() == 1 ?
                roots.getFirst() :
                new Mu(roots);
    }

    private class DecompositionVisitor extends ReturningOpBaseVisitor {
        private Map<OpService, Node> group2source = new HashMap<>();

        public Op visit(OpService opService) {
            OpService unboundService = new OpService(Var.alloc("__groupvar" + counter++),opService.getSubOp(), opService.getSilent());
            group2source.put(unboundService, opService.getService());

            return unboundService;
        }
    }

    private class RemoveAssignsAndExtends extends ReturningOpBaseVisitor {
        public Op visit(OpAssign opAssign) {
            return ReturningOpVisitorRouter.visit(this, opAssign.getSubOp());
        }
        public Op visit(OpExtend opExtend) {
            if(opExtend.getSubOp() instanceof OpService opService) {
                // if the extend op is over a service whose URI is bound, then we remove the extend op, it's superfluous
                if(opService.getService().isURI()){
                    return ReturningOpVisitorRouter.visit(this, opExtend.getSubOp());
                }
            }
            return ReturningOpVisitorRouter.visit(this, opExtend.getSubOp());
        }
    }
}
