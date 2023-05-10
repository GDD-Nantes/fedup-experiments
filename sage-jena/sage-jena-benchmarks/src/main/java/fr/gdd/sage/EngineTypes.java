package fr.gdd.sage;

/**
 * The different query engines that can be tested.
 **/
public interface EngineTypes {
    String TDB = "TDB";
    String Sage = "Sage";
    String SageTimeout60s = "Sage 60s";
    String SageTimeout1s = "Sage 1s";


    String SageForceOrder = "Sage force order";
    String TDBForceOrder = "TDB force order";

    String SageForceOrderTimeout60s = "Sage force order 60s";
    String SageForceOrderTimeout30s = "Sage force order 30s";
    String SageForceOrderTimeout10s = "Sage force order 10s";
    String SageForceOrderTimeout1s  = "Sage force order 1s";
    String SageForceOrderTimeout1ms  = "Sage force order 1ms";

    String SageForceOrderLimit1 = "Sage force order limit 1";
}
