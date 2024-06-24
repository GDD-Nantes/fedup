package fr.gdd.fedup.fuseki;

import org.apache.jena.sparql.util.Symbol;

/**
 * Register the constant of FedUP that are useful in fuseki, or graph, or execution
 * context.
 */
public class FedUPConstants {

    public static final Symbol EXECUTION_ENGINE = Symbol.create("FedUP_ExecutionEngine");
    public static final Symbol EXPORT_PLANS = Symbol.create("FedUP_ExportPlans");
    public static final Symbol MODIFY_ENDPOINTS = Symbol.create("FedUP_ModifyEndpoints");

    public static final String APACHE_JENA = "Jena";
    public static final String FEDX = "FedX";

    public static final Symbol EXPORTED = Symbol.create("FedUP_Exported");
}
