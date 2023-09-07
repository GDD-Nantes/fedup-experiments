package fr.univnantes.gdd.fedup;

import org.apache.jena.sparql.util.Symbol;

/**
 * List of symbols important for defining a federation.
 */
public class FedUPConstants {
    public static final String systemVarNS = "https://sage.gdd.fr/FedUP#";
    public static final String fedUPSymbolPrefix = "RAW";

    public static Symbol federation = allocConstantSymbol("Federation");
    public static Symbol summary    = allocConstantSymbol("Summary");

    public static Symbol allocConstantSymbol(String name) {
        return Symbol.create(systemVarNS + name);
    }
    public static Symbol allocVariableSymbol(String name) {
        return Symbol.create(fedUPSymbolPrefix + name);
    }

}
