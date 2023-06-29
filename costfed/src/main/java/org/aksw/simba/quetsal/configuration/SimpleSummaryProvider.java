package org.aksw.simba.quetsal.configuration;

import java.util.List;

import com.fluidops.fedx.Config;
import com.fluidops.fedx.FedX;
import com.fluidops.fedx.Summary;
import com.fluidops.fedx.SummaryProvider;

public class SimpleSummaryProvider implements SummaryProvider {

    @Override
    public Summary getSummary(FedX federation) {
        Config config = federation.getConfig();
        String path = config.getProperty("quetzal.fedSummaries");
        return new CostFedSummary(List.of(path));
    }

    @Override
    public void close() {
        // do nothing
    }
    
}

