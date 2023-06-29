package org.aksw.simba.quetsal.configuration;

import com.fluidops.fedx.Config;

public class HibiscusQuetzalConfig extends QuetzalConfig {
    
    public HibiscusQuetzalConfig(Config config) {
        super(config, "http://aksw.org/fedsum/");
    }
}
