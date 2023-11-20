package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Builds a JSON version of the plan.
 */
public class FedQPL2JSONVisitor implements FedQPLVisitor<ObjectUtils.Null> {

    JsonObjectBuilder json;
    JsonObjectBuilder current_builder;

    public FedQPL2JSONVisitor() {
        json = Json.createObjectBuilder();
        current_builder = json;
    }

    public ObjectUtils.Null visit(Mu mu) {
        JsonObjectBuilder json_children = Json.createObjectBuilder();
        current_builder.add(mu.getClass().getSimpleName(), json_children);

        for (FedQPLOperator child : mu.getChildren()) {
            JsonObjectBuilder child_builder = Json.createObjectBuilder();
            json_children.add(child.getClass().getSimpleName(), child_builder);
            current_builder = child_builder;
            child.visit(this);
        }
        return ObjectUtils.NULL;
    }

    public ObjectUtils.Null visit(Mj mj) {
        JsonObjectBuilder json_children = Json.createObjectBuilder();
        current_builder.add(mj.getClass().getSimpleName(), json_children);

        for (FedQPLOperator child : mj.getChildren()) {
            JsonObjectBuilder child_builder = Json.createObjectBuilder();
            json_children.add(child.getClass().getSimpleName(), child_builder);
            current_builder = child_builder;
            child.visit(this);
        }
        return ObjectUtils.NULL;
    }

    public ObjectUtils.Null visit(Req req) {
        current_builder.addNull(req.getClass().getSimpleName());
        return ObjectUtils.NULL;
    }

    public ObjectUtils.Null visit(LeftJoin lj) {
        JsonObjectBuilder join_builder = Json.createObjectBuilder();
        current_builder.add(lj.getClass().getSimpleName(), join_builder);
        JsonObjectBuilder left_builder = Json.createObjectBuilder();
        join_builder.add("left", left_builder);
        current_builder = left_builder;
        lj.getLeft().visit(this);

        JsonObjectBuilder right_builder = Json.createObjectBuilder();
        join_builder.add("right", right_builder);
        current_builder = right_builder;
        lj.getRight().visit(this);
        return ObjectUtils.NULL;
    }

    public ObjectUtils.Null visit(Filter filter) {
        JsonObjectBuilder filter_builder = Json.createObjectBuilder();
        current_builder.add(filter.getClass().getSimpleName(), filter_builder);

        JsonObjectBuilder subOpBuilder = Json.createObjectBuilder();
        subOpBuilder.add(filter.getSubOp().getClass().getSimpleName(), subOpBuilder);
        current_builder = subOpBuilder;
        filter.getSubOp().visit(this);
        return ObjectUtils.NULL;
    }

}
