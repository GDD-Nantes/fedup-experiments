package fr.gdd.sage.arq;

import org.apache.jena.sparql.util.Symbol;


/**
 * Enables quick access to variables that are stored in the context.
 **/
public class SageConstants {

    public static final String systemVarNS = "https://sage.gdd.fr/Sage#";
    public static final String sageSymbolPrefix = "sage";

    static public Symbol timeout = allocConstantSymbol("Timeout");
    static public Symbol limit   = allocConstantSymbol("Limit");
    static public Symbol state   = allocConstantSymbol("State");

    static public Symbol scanFactory = allocVariableSymbol("ScanFactory");
    static public Symbol output   = allocVariableSymbol("Output");
    static public Symbol input    = allocVariableSymbol("Input");

    static public Symbol cursor = allocVariableSymbol("Cursor");
    
    /**
     * Symbol in use in the global context.
     */
    public static Symbol allocConstantSymbol(String name) {
        return Symbol.create(systemVarNS + name);
    }
    
    /**
     * Symbol in use in each execution context.
     */
    public static Symbol allocVariableSymbol(String name) {
        return Symbol.create(sageSymbolPrefix + name);
    }
}
