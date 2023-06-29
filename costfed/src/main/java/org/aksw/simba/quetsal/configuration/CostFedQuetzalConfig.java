package org.aksw.simba.quetsal.configuration;

import com.fluidops.fedx.Config;

public class CostFedQuetzalConfig extends QuetzalConfig {
    
    public CostFedQuetzalConfig(Config config) {
        super(config, "http://aksw.org/quetsal/");
    }
}
