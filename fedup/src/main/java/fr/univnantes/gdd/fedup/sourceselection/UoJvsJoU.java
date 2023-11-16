package fr.univnantes.gdd.fedup.sourceselection;

import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.algebra.StatementSource;
import fr.univnantes.gdd.fedup.Spy;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UoJvsJoU {

    private final FedXConnection connection;

    public UoJvsJoU(FedXConnection connection) {
        this.connection = connection;
    }

    private List<Map<StatementPattern, List<StatementSource>>> generateJoUFromUoJ(List<Map<StatementPattern, List<StatementSource>>> uoj) {
        Map<StatementPattern, List<StatementSource>> jou = new HashMap<>();
        for (Map<StatementPattern, List<StatementSource>> uojUnit : uoj) {
            for (StatementPattern statementPattern : uojUnit.keySet()) {
                if (!jou.containsKey(statementPattern)) {
                    jou.put(statementPattern, new ArrayList<>());
                }
                for (StatementSource source : uojUnit.get(statementPattern)) {
                    if (!jou.get(statementPattern).contains(source)) {
                        jou.get(statementPattern).add(source);
                    }
                }
            }
        }
        return List.of(jou);
    }

    public List<Map<StatementPattern, List<StatementSource>>> selectBestAssignment(String queryString, List<Map<StatementPattern, List<StatementSource>>> uoj) throws Exception {
        int uojCost = SACost.compute(this.connection, queryString, uoj).stream().reduce(0, Integer::sum);

        List<Map<StatementPattern, List<StatementSource>>> jou = this.generateJoUFromUoJ(uoj);
        int jouCost = SACost.compute(this.connection, queryString, jou).stream().reduce(0, Integer::sum);

        if (uojCost <= jouCost) {
            return uoj;
        } else {
            return jou;
        }
    }
}
