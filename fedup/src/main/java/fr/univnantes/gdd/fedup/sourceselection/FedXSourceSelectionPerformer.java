package fr.univnantes.gdd.fedup.sourceselection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import com.fluidops.fedx.algebra.EmptyStatementPattern;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.optimizer.DefaultSourceSelection;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;
import com.fluidops.fedx.structures.SubQuery;

import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.Utils;

public class FedXSourceSelectionPerformer extends SourceSelectionPerformer {

    private static final Logger logger = LogManager.getLogger(FedXSourceSelectionPerformer.class);

    public FedXSourceSelectionPerformer(SailRepositoryConnection connection) {
        super(connection);
    }

    @Override
    public List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(String queryString) throws Exception {
        logger.info("Source selection computed using FedX");
        
        List<Endpoint> endpoints = connection.getEndpoints();
        Cache cache = connection.getFederation().getCache();
        QueryInfo queryInfo = new QueryInfo(connection, queryString, QueryType.SELECT, null);

        cache.clear();

        SourceSelection sourceSelection = new SourceSelection(endpoints, cache, queryInfo);

        long startTime = System.currentTimeMillis();
        sourceSelection.performSourceSelection(Utils.getBasicGraphPatterns(queryString));
        long endTime = System.currentTimeMillis();

        Spy.getInstance().sourceSelectionTime = endTime - startTime;

        return List.of(sourceSelection.getStmtToSources());
    }
 
    private static class SourceSelection extends DefaultSourceSelection {

        public SourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
            super(endpoints, cache, queryInfo);
        }
        
        @Override
        public void performSourceSelection(List<List<StatementPattern>> bgpGroups) {
            stmtToSources = new ConcurrentHashMap<StatementPattern, List<StatementSource>>();
            List<CheckTaskPair> remoteCheckTasks = new ArrayList<CheckTaskPair>();
            
            for (List<StatementPattern> stmts : bgpGroups) {
                for (StatementPattern stmt : stmts) {
                    stmtToSources.put(stmt, new ArrayList<StatementSource>());
                    SubQuery subQuery = new SubQuery(stmt);                        
                    for (Endpoint endpoint : endpoints) {
                        StatementSourceAssurance assurance = cache.canProvideStatements(subQuery, endpoint);
                        if (assurance == StatementSourceAssurance.HAS_LOCAL_STATEMENTS) {
                            addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.LOCAL));
                        } else if (assurance == StatementSourceAssurance.HAS_REMOTE_STATEMENTS) {
                            addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));			
                        } else if (assurance == StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) {					
                            remoteCheckTasks.add(new CheckTaskPair(endpoint, stmt));
                            Spy.getInstance().numASKQueries += 1;
                        }
                    }
                }
            }
            
            if (!remoteCheckTasks.isEmpty()) {
                SourceSelectionExecutorWithLatch.run(queryInfo.getFederation().getScheduler(), this, remoteCheckTasks, cache);
            }
                    
            for (StatementPattern stmt : stmtToSources.keySet()) {
                List<StatementSource> sources = stmtToSources.get(stmt);
                if (sources.size()>1) {
                    StatementSourcePattern stmtNode = new StatementSourcePattern(stmt, queryInfo);
                    for (StatementSource source : sources) {
                        stmtNode.addStatementSource(source);
                    }
                    stmt.replaceWith(stmtNode);
                } else if (sources.size() == 1) {
                    stmt.replaceWith(new ExclusiveStatement(stmt, sources.get(0), queryInfo));
                } else {
                    stmt.replaceWith(new EmptyStatementPattern(stmt));
                }
            }		
        }
    }
}
