package fr.gdd.sage.configuration;

import java.util.Map;
import java.util.Objects;

import fr.gdd.sage.io.SageInput;



/**
 * Builder to ease the creation of sage input.
 *
 * Global configurations have more priority than local ones,
 * eg. global represents server wide configuration while local
 * represents a user defined preference.
 **/
public class SageInputBuilder {

    SageServerConfiguration global = new SageServerConfiguration();
    SageInput<?> local = new SageInput<>();


    
    public SageInputBuilder() { }

    public SageInputBuilder globalConfig(SageServerConfiguration global) {
        this.global = Objects.nonNull(global) ? global : this.global;
        return this;
    }

    public SageInputBuilder localInput(SageInput<?> local) {
        this.local = Objects.nonNull(local) ? local : this.local;
        return this;
    }

    public SageInput<?> build() {
        long timeout = Math.min(global.getTimeout(), local.getTimeout());
        long limit   = Math.min(global.getLimit(), local.getLimit());
        
        return new SageInput<>()
            .setBackjumping(local.isBackjumping())
            .setRandomWalking(local.isRandomWalking())
            .setTimeout(timeout)
            .setDeadline(Long.MAX_VALUE == timeout ? timeout : System.currentTimeMillis() + timeout)
            .setLimit(limit)
            .setState((Map)local.getState());
    }

}
