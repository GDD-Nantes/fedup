package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import fr.gdd.fedqpl.operators.Filter;
import fr.gdd.fedqpl.operators.LeftJoin;
import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.operators.Req;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;

public class PrinterVisitor extends FedQPLVisitor {

    JsonObjectBuilder json;
    JsonObjectBuilder current_builder;

    public PrinterVisitor() {
        json = Json.createObjectBuilder();
        current_builder = json;
    }

    public void visit(Mu mu) {
        JsonObjectBuilder json_children = Json.createObjectBuilder();
        current_builder.add(mu.getName(), json_children);

        for (FedQPLOperator child : mu.getChildren()) {
            JsonObjectBuilder child_builder = Json.createObjectBuilder();
            json_children.add(child.getName(), child_builder);
            current_builder = child_builder;
            child.visit(this);
        }
    }

    public void visit(Mj mj) {
        JsonObjectBuilder json_children = Json.createObjectBuilder();
        current_builder.add(mj.getName(), json_children);

        for (FedQPLOperator child : mj.getChildren()) {
            JsonObjectBuilder child_builder = Json.createObjectBuilder();
            json_children.add(child.getName(), child_builder);
            current_builder = child_builder;
            child.visit(this);
        }
    }

    public void visit(Req req) {
        current_builder.addNull(req.getName());
    }

    public void visit(LeftJoin lj) {
        JsonObjectBuilder join_builder = Json.createObjectBuilder();
        current_builder.add(lj.getName(), join_builder);
        JsonObjectBuilder left_builder = Json.createObjectBuilder();
        join_builder.add("left", left_builder);
        current_builder = left_builder;
        lj.getLeft().visit(this);

        JsonObjectBuilder right_builder = Json.createObjectBuilder();
        join_builder.add("right", right_builder);
        current_builder = right_builder;
        lj.getRight().visit(this);
    }

    public void visit(Filter filter) {
        JsonObjectBuilder filter_builder = Json.createObjectBuilder();
        current_builder.add(filter.getName(), filter_builder);

        JsonObjectBuilder subOpBuilder = Json.createObjectBuilder();
        subOpBuilder.add(filter.getSubOp().getName(), subOpBuilder);
        current_builder = subOpBuilder;
        filter.getSubOp().visit(this);
    }

    public void pprint() {
        System.out.println(json.build().toString());
    }
}
