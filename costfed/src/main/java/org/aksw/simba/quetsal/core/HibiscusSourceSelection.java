package org.aksw.simba.quetsal.core;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.aksw.simba.quetsal.configuration.QuetzalConfig;
import org.aksw.simba.quetsal.configuration.QuetzalConfig.Mode;
import org.aksw.simba.quetsal.configuration.Summary;
import org.aksw.simba.quetsal.datastructues.HyperGraph.HyperEdge;
import org.aksw.simba.quetsal.datastructues.HyperGraph.Vertex;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fluidops.fedx.algebra.EmptyStatementPattern;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.cache.CacheEntry;
import com.fluidops.fedx.cache.CacheUtils;
import com.fluidops.fedx.evaluation.TripleSource;
import com.fluidops.fedx.evaluation.concurrent.ControlledWorkerScheduler;
import com.fluidops.fedx.exception.ExceptionUtil;
import com.fluidops.fedx.exception.OptimizationException;
import com.fluidops.fedx.optimizer.SourceSelection;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.SubQuery;
import com.fluidops.fedx.util.QueryStringUtil;

/**
 * Perform triple pattern-wise source selection using FedSumaries, cache or SPARQL ASK. 
 * Note that this is first phase of our source selection. In second phase we perform source filtering for each of the statement pattern using Hyper Graphs 
 * @author Saleem
 */
public class HibiscusSourceSelection extends SourceSelection {
    final static Logger log = LoggerFactory.getLogger(HibiscusSourceSelection.class);
	
	public Map<HyperEdge,StatementPattern> hyperEdgeToStmt = new HashMap<HyperEdge,StatementPattern>(); // Hyper edges to Triple pattern Map
	public List<Set<Vertex>> theDNFHyperVertices = new ArrayList<Set<Vertex>>(); // Sets of vertices in different DNF hypergraphs
	
	final QuetzalConfig quetzalConfig;
		
	public HibiscusSourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
		super(endpoints, cache, queryInfo);
		quetzalConfig = queryInfo.getFederation().getConfig().getExtension();
	}
	
	RepositoryConnection getSummaryConnection() {
	    return ((Summary)(queryInfo.getFedXConnection().getSummary())).getConnection();
	}
	   
	public List<CheckTaskPair> remoteCheckTasks = new ArrayList<CheckTaskPair>();

	/**
	 * Perform triple pattern-wise source selection for the provided statement patterns of a SPARQL query using HiBISCuS Summaries, cache or remote ASK queries.
	 *Remote ASK queries are evaluated in parallel using the concurrency infrastructure of FedX. Note,
	 * that this method is blocking until every source is resolved.
	 * Recent SPARQL ASK operations are cached for future use.
	 * The statement patterns are replaced by appropriate annotations in this process.
	 * Hypergraphs are created in step 1 of source selection and are used in step 2 i.e. prunning of 
	 * triple pattern-wise selected sources. 
	 * @param DNFgrps Set of BGP's in SPARQL Query
	 * @return stmtToSources Map of triple patterns to relevant sources
	 */
	@Override
	public void performSourceSelection(List<List<StatementPattern>> bgpGroups) {
		// Map statements to their sources. Use synchronized access!
		stmtToSources = new ConcurrentHashMap<StatementPattern, List<StatementSource>>();
		
		long tp = 0 ;
		for(List<StatementPattern> stmts : bgpGroups) {
			Set<Vertex> V = new HashSet<Vertex>();
			for (StatementPattern stmt : stmts) {
				tp++;
				//cache.clear();
				stmtToSources.put(stmt, new ArrayList<StatementSource>());
				String s, p, o, sa = "null", oa = "null", sbjVertexLabel, objVertexLabel, predVertexLabel;
				Vertex sbjVertex, predVertex, objVertex;
				if (stmt.getSubjectVar().getValue() != null) {
					s = stmt.getSubjectVar().getValue().stringValue();
					String[] sbjPrts = s.split("/");
					sa = sbjPrts[0] + "//" + sbjPrts[2];
					sbjVertexLabel = s;
					sbjVertex = new Vertex(stmt.getSubjectVar(), sbjVertexLabel);
					if (!vertexExist(sbjVertex, V)) {
						V.add(sbjVertex);
					}
				} else {
					s = "null";  
					sbjVertexLabel = stmt.getSubjectVar().getName();
					sbjVertex = new Vertex(stmt.getSubjectVar(), sbjVertexLabel);
					if(!vertexExist(sbjVertex, V)) {
						V.add(sbjVertex);
					}
				}
				if (stmt.getPredicateVar().getValue() != null) {
					p = stmt.getPredicateVar().getValue().stringValue();
					predVertexLabel = p;
					predVertex = new Vertex(stmt.getPredicateVar(), predVertexLabel);
					if(!vertexExist(predVertex, V)) {
						V.add(predVertex);
					}
				} else {
					p = "null"; 
					predVertexLabel = stmt.getPredicateVar().getName();
					predVertex= new Vertex(stmt.getPredicateVar(), predVertexLabel);
					if(!vertexExist(predVertex, V)) {
						V.add(predVertex);
					}
				}
				if (stmt.getObjectVar().getValue() != null) {
					o = stmt.getObjectVar().getValue().stringValue();
					String[] objPrts = o.split("/");
					if ((objPrts.length > 1)) {
						oa =objPrts[0] + "//" + objPrts[2];
					} 
					else {
						oa = "null";
					}
					objVertexLabel = o;
					objVertex = new Vertex(stmt.getObjectVar(), objVertexLabel);
					if(!vertexExist(objVertex, V)) {
						V.add(objVertex);
					}
				} else {
					o = "null"; 
					objVertexLabel = stmt.getObjectVar().getName();
					objVertex= new Vertex(stmt.getObjectVar(), objVertexLabel);
					if(!vertexExist(objVertex, V)) {
						V.add(objVertex);
					}
				}

				if (quetzalConfig.mode == Mode.ASK_DOMINANT) {
					if(s.equals("null") && p.equals("null") && o.equals("null")) {
						for (Endpoint e : endpoints) {
							addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
						}
					} else if (!p.equals("null")) {
						if(p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && !o.equals("null")) {
							FedSumClassLookup(stmt, p, o);
						} else if (!quetzalConfig.commonPredicates.contains(p) || (s.equals("null") && o.equals("null"))) {
							FedSumLookup(stmt, sa, p, oa);
						} else {
							cache_ASKselection(stmt);    
						}
					} else {
						cache_ASKselection(stmt);
					}		
				} else {
					if(s.equals("null") && p.equals("null") && o.equals("null")) {
						for (Endpoint e : endpoints) {
							addSource(stmt, new StatementSource(e.getId(), StatementSourceType.REMOTE));
						}
					} else if (!s.equals("null") || !p.equals("null")) {
						if(!p.equals("null") && p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && !o.equals("null")) {
							FedSumClassLookup(stmt, p, o);
						} else {
							FedSumLookup(stmt, sa, p, oa);
						}		
					} else {
						cache_ASKselection(stmt);
					}	
				}

				HyperEdge hEdge = new HyperEdge(sbjVertex,predVertex,objVertex);

				if(!(getVertex(sbjVertexLabel, V) == null)) {
					sbjVertex = getVertex(sbjVertexLabel, V);
				}	
				if(!(getVertex(predVertexLabel, V) == null)) {
					predVertex = getVertex(predVertexLabel, V);
				}
				if(!(getVertex(objVertexLabel, V) == null)) {
					objVertex = getVertex(objVertexLabel, V);
				}
				sbjVertex.outEdges.add(hEdge);
				predVertex.inEdges.add(hEdge);
				objVertex.inEdges.add(hEdge);
				hyperEdgeToStmt.put(hEdge, stmt);			
			}
			theDNFHyperVertices.add(V);
		}

		if (remoteCheckTasks.size() > 0) {
			SourceSelectionExecutorWithLatch.run(queryInfo.getFederation().getScheduler(), this, remoteCheckTasks, cache);
			System.out.println("Number of ASK request: " + remoteCheckTasks.size());
		} else {
			System.out.println("Number of ASK request: 0");
		}

		int triplePatternWiseSources = 0 ;
		for (StatementPattern stmt : stmtToSources.keySet()) {
			// System.out.println("sources for " + stmt + " = " + stmtToSources.get(stmt));
			triplePatternWiseSources = triplePatternWiseSources + stmtToSources.get(stmt).size();
		}

		if (triplePatternWiseSources > tp) {
			// System.out.println("start pruning");
			stmtToSources = pruneSources(theDNFHyperVertices);
		}
		
	    // int triplePatternWiseSources = 0 ;
		for (StatementPattern stmt : stmtToSources.keySet()) {
			// System.out.println("sources for " + stmt + " = " + stmtToSources.get(stmt));
			List<StatementSource> sources = stmtToSources.get(stmt);
			// triplePatternWiseSources = triplePatternWiseSources + sources.size();

			if (sources.size() > 1) {
				StatementSourcePattern stmtNode = new StatementSourcePattern(stmt, queryInfo);
				for (StatementSource s : sources) {
					stmtNode.addStatementSource(s);
				}
				stmt.replaceWith(stmtNode);
			} else if (sources.size() == 1) {
				stmt.replaceWith(new ExclusiveStatement(stmt, sources.get(0), queryInfo));
			} else {
				// if (log.isDebugEnabled()) {
				// 	log.debug("Statement " + QueryStringUtil.toString(stmt) + " does not produce any results at the provided sources, replacing node with EmptyStatementPattern." );
				// }	
				stmt.replaceWith(new EmptyStatementPattern(stmt));
			}
		}
	}

	/**
	 * Retrieve a vertex having a specific label from a set of Vertrices
	 * @param label Label of vertex to be retrieved
	 * @param V Set of vertices
	 * @return Vertex if exist otherwise null
	 */
	public Vertex getVertex(String label, Set<Vertex> V) {
		for(Vertex v : V) {
			if(v.label.equals(label)) {
				return v;
			}
		}
		return null;
	}

	/**
	 * Check if a  vertex already exists in set of all vertices
	 * @param sbjVertex Subject Vertex
	 * @param V Set of all vertices
	 * @return value Boolean value
	 */
	public boolean vertexExist(Vertex sbjVertex, Set<Vertex> V) {
		for(Vertex v : V) {
			if(sbjVertex.label.equals(v.label)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Use cache and SPARQL ASK to perform relevant source selection for a statement pattern
	 * @param stmt statement pattern
	 */
	public void cache_ASKselection(StatementPattern stmt) {
		SubQuery subQuery = new SubQuery(stmt);
		for (Endpoint endpoint : endpoints) {
			StatementSourceAssurance assurance = cache.canProvideStatements(subQuery, endpoint);
			if (assurance == StatementSourceAssurance.HAS_LOCAL_STATEMENTS) {
				addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.LOCAL));
			} else if (assurance == StatementSourceAssurance.HAS_REMOTE_STATEMENTS) {
				addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));
			} else if (assurance == StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) {
				remoteCheckTasks.add(new CheckTaskPair(endpoint, stmt));
			}
		}
	}

	/**
	 * Search HiBISCuS index for the given triple pattern p with sbj authority and obj authority.
	 * Note: sa, oa can be null i.e. for unbound tuple 
	 * @param stmt Statement pattern	
	 * @param sa Subject authority
	 * @param p Predicate
	 * @param oa Object authority
	 * @throws QueryEvaluationException Query Error
	 * @throws MalformedQueryException  Memory Error
	 * @throws RepositoryException  Repository Erro
	 */
	public void FedSumLookup(StatementPattern stmt, String sa, String p, String oa) throws QueryEvaluationException, RepositoryException, MalformedQueryException {
		String queryString = getFedSumLookupQuery(sa,p,oa) ;
		TupleQuery tupleQuery = getSummaryConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while(result.hasNext()) {
			String endpoint = result.next().getValue("url").stringValue();
			String id = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
			addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
		}
	}

	/**
	 * Get SPARQL query for index lookup
	 * @param sa Subject Authority
	 * @param p Predicate
	 * @param oa Object Authority
	 * @return queryString Query String
	 */
	public String getFedSumLookupQuery(String sa, String p, String oa) {
		String queryString = null;
		if(!p.equals("null")) {
			if(sa.equals("null") && oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?url WHERE {"
					+ "    ?s ds:url ?url ."
					+ "    ?s ds:capability ?cap ."
					+ "	   ?cap ds:predicate <" + p + "> ."
					+ "}";
			} else if (!sa.equals("null") && !oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?url WHERE {"
					+ "    ?s ds:url ?url ."
					+ "    ?s ds:capability ?cap ."
					+ "	   ?cap ds:predicate <" + p + "> ."
					+ "    ?cap ds:sbjAuthority <" + sa + "> ."
					+ "    ?cap ds:objAuthority <" + oa + "> ."
					+ "}";	
			} else if (!sa.equals("null") && oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?url WHERE {"
					+ "    ?s ds:url ?url ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:predicate <" + p + "> ."
					+ "    ?cap ds:sbjAuthority <" + sa + "> ."
					+ "}";	
			} else if (sa.equals("null") && !oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?url WHERE {"
					+ "    ?s ds:url ?url ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:predicate <" + p + "> ."
					+ "    ?cap ds:objAuthority <" + oa + "> ."
					+ "}";	
			}
		} else {
			if (!sa.equals("null") && !oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?url WHERE {"
					+ "    ?s ds:url ?url ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:sbjAuthority <" + sa + "> ."
					+ "    ?cap ds:objAuthority <" + oa + "> ."
					+ "}";	
			} else if (!sa.equals("null") && oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?url WHERE {"
					+ "    ?s ds:url ?url ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:sbjAuthority <" + sa + "> ."
					+ "}";	
			} else if (sa.equals("null") && !oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?url WHERE {"
					+ "    ?s ds:url ?url ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:objAuthority <" + oa + "> ."
					+ "}";	
			}
		}
		return queryString;
	}

	/**
	 * HiBISCuS Index lookup for rdf:type and its its corresponding values
	 * @param p Predicate i.e. rdf:type
	 * @param o Predicate value
	 * @param stmt Statement Pattern
	 * @throws RepositoryException Repository Error
	 * @throws MalformedQueryException Query Error
	 * @throws QueryEvaluationException Query Execution Error
	 */
	public void FedSumClassLookup(StatementPattern stmt, String p, String o) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		String queryString = "PREFIX ds:<http://aksw.org/fedsum/> "
			+ "SELECT DISTINCT ?url WHERE {"
			+ "    ?s ds:url ?url ."
			+ "    ?s ds:capability ?cap ."
			+ "	   ?cap ds:predicate <" + p + "> ."
			+ "    ?cap ds:objAuthority  <" + o + "> ."
			+ "}";
		TupleQuery tupleQuery = getSummaryConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while(result.hasNext()) {
			String endpoint = result.next().getValue("url").stringValue();
			String id = "sparql_" + endpoint.replace("http://", "").replace("/", "_");
			addSource(stmt, new StatementSource(id, StatementSourceType.REMOTE));
		}
	}

	//------------------------------------------------------------
	/**
	 * Retrieve a set of relevant sources for this query.
	 * @return endpoints set of relevant sources
	 */
	public Set<Endpoint> getRelevantSources() {
		Set<Endpoint> endpoints = new HashSet<Endpoint>();
		for (List<StatementSource> sourceList : stmtToSources.values()) {
			for (StatementSource source : sourceList) {
				endpoints.add( queryInfo.getFedXConnection().getEndpointManager().getEndpoint(source.getEndpointID()));
			}
		}
		return endpoints;
	}	
	
	/**
	 * Add a source to the given statement in the map (synchronized through map)
	 * 
	 * @param stmt Triple Pattern
	 * @param source Source or SPARQL endpoint
	 */
	public void addSource(StatementPattern stmt, StatementSource source) {
		// The list for the stmt mapping is already initialized
		List<StatementSource> sources = stmtToSources.get(stmt);
		synchronized (sources) {
			sources.add(source);
		}
	}

	/**
	 * Step 2 of HiBISCuS source selection. i.e. triple pattern-wise selected sources for hyperedge aka triple pattern
	 * @param dNFHyperVertices DNF groups (BGPs)of hypervertices
	 * @return Refine triple pattern-wise selected sources
	 */
	public Map<StatementPattern, List<StatementSource>> pruneSources(List<Set<Vertex>> dNFHyperVertices) {
		for (Set<Vertex> V : dNFHyperVertices) {
			if(V.size() > 3) {
				for (Vertex v : V) {
					Map <StatementPattern, Map<StatementSource, ArrayList<String>>> stmtToLstAuthorities = new HashMap<>(); 
					ArrayList<String> authIntersectionSet = new ArrayList<String>();
					if((v.inEdges.size() > 1 && v.outEdges.size() > 0) || (v.inEdges.size() > 0 && v.outEdges.size() > 1)) { // hybrid node
						// System.out.println("pruning hybrid node: " + v);
						for(HyperEdge inEdge : v.inEdges) {
							Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<>();
							ArrayList<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt = hyperEdgeToStmt.get(inEdge);
							for (StatementSource src : stmtToSources.get(stmt)) {
								ArrayList<String> lstAuthorities = FedSumD_getMatchingObjAuthorities(stmt, src, v);  //has authorities
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
					 		if(authIntersectionSet.size() == 0) {
								authIntersectionSet = authUnionSet;
							} else {
								authIntersectionSet.retainAll(authUnionSet);
							}
					    	stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
				 		}
						for(HyperEdge outEdge : v.outEdges) {
							Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<>();
							ArrayList<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt =  hyperEdgeToStmt.get(outEdge);
						 	for (StatementSource src:stmtToSources.get(stmt)) { //has relevant sources
								ArrayList<String> lstAuthorities =  FedSumD_getMatchingSbjAuthorities(stmt,src);  //has authorities
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
						 	if(authIntersectionSet.size() == 0) {
								authIntersectionSet = authUnionSet;
							} else {
								authIntersectionSet.retainAll(authUnionSet);
							}
						    stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
						}
					 	doSourcePrunning(stmtToLstAuthorities, authIntersectionSet);
			 		} else if(v.outEdges.size() > 1) { // star node
						// System.out.println("pruning star node: " + v);
						for(HyperEdge outEdge : v.outEdges) {
							Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<>();
							ArrayList<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt = hyperEdgeToStmt.get(outEdge);
							// System.out.println("outgoing edge: " + stmt);
							// System.out.println("sources: " + stmtToSources.get(stmt));
							for (StatementSource src : stmtToSources.get(stmt)) {
								ArrayList<String> lstAuthorities = FedSumD_getMatchingSbjAuthorities(stmt, src);  //has authorities
								// System.out.println("retrieving subject authorities for source " + src);
								// System.out.println("authorities: " + lstAuthorities);
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
							if(authIntersectionSet.size() == 0) {
								authIntersectionSet = authUnionSet;
							} else {
								authIntersectionSet.retainAll(authUnionSet);
							}
							stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
						}
						// System.out.println("----------");
						// System.out.println(stmtToLstAuthorities);
						// System.out.println(authIntersectionSet);
						// System.out.println("----------");
						doSourcePrunning(stmtToLstAuthorities, authIntersectionSet);
					} else if(v.outEdges.size() == 1 && v.inEdges.size() == 1) { // path node
						// System.out.println("pruning path node: " + v);
						for(HyperEdge inEdge : v.inEdges) {
							Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<>();
							ArrayList<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt = hyperEdgeToStmt.get(inEdge);
							// System.out.println("ingoing edge: " + stmt);
							// System.out.println("sources: " + stmtToSources.get(stmt));
							for (StatementSource src : stmtToSources.get(stmt)) {
								ArrayList<String> lstAuthorities =  FedSumD_getMatchingObjAuthorities(stmt, src, v);  //has authorities
								// System.out.println("retrieving subject authorities for source " + src);
								// System.out.println("authorities: " + lstAuthorities);
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
					 		}
					 		if(authIntersectionSet.size() == 0) {
								authIntersectionSet = authUnionSet;
							} else {
								authIntersectionSet.retainAll(authUnionSet);
							}	
					    	stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
				 		}
						for(HyperEdge outEdge : v.outEdges) {
							Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<StatementSource, ArrayList<String>> ();
							ArrayList<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt = hyperEdgeToStmt.get(outEdge);
							// System.out.println("outgoing edge: " + stmt);
							// System.out.println("sources: " + stmtToSources.get(stmt));
							for (StatementSource src: stmtToSources.get(stmt)) {
								ArrayList<String> lstAuthorities =  FedSumD_getMatchingSbjAuthorities(stmt, src);  //has authorities
								// System.out.println("retrieving subject authorities for source " + src);
								// System.out.println("authorities: " + lstAuthorities);
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
							}
							if(authIntersectionSet.size() == 0) {
								authIntersectionSet = authUnionSet;
							} else {
								authIntersectionSet.retainAll(authUnionSet);
							}		
						    stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities); 
						}
					 	doSourcePrunning(stmtToLstAuthorities, authIntersectionSet);
					 } else if(v.inEdges.size() > 1 && v.outEdges.size() == 0) { // sink node
						// System.out.println("pruning sink node: " + v);
			 			outerloop: for(HyperEdge inEdge : v.inEdges) {
							Map<StatementSource, ArrayList<String>> stmtSourceToAuthorities = new HashMap<>();
							ArrayList<String> authUnionSet = new ArrayList<String>();
							StatementPattern stmt = hyperEdgeToStmt.get(inEdge);
							for (StatementSource src : stmtToSources.get(stmt)) {
								ArrayList<String> lstAuthorities = FedSumD_getMatchingObjAuthorities(stmt,src,v);  //has authorities
								if (lstAuthorities.isEmpty()) {
									break outerloop;
								}	
								authUnionSet = getUnion(authUnionSet, lstAuthorities);
								stmtSourceToAuthorities.put(src, lstAuthorities);
					 		}
							if(authIntersectionSet.size() == 0) {
								authIntersectionSet = authUnionSet;
							} else {
								authIntersectionSet.retainAll(authUnionSet);
							}
					    	stmtToLstAuthorities.put(stmt, stmtSourceToAuthorities);
				 		}				
					 	doSourcePrunning(stmtToLstAuthorities,authIntersectionSet);
				 	}	 
		 		}
		 	}
		}
		return stmtToSources;
	}

	/**
	 *  Union of two Sets
	 * @param authUnionSet First Set
	 * @param lstAuthorities Second Set
	 * @return Union of two sets
	 */
	public ArrayList<String> getUnion(ArrayList<String> authUnionSet, ArrayList<String> lstAuthorities) {
		for(String authority:lstAuthorities) {
			if(!authUnionSet.contains(authority)) {
				authUnionSet.add(authority);
			}
		}
		return authUnionSet;
	}

	/**
	 * Remove irrelvant sources from each triple pattern according to step-2 of our source selection
	 * @param stmtToLstAuthorities A map which stores the list of authorities for each capable source of a triple pattern
	 * @param authIntersectionSet The common authorities set. see step 2 at FedSum paper for the usage of this list
	 */
	private void doSourcePrunning(Map<StatementPattern, Map<StatementSource, ArrayList<String>>> stmtToLstAuthorities, ArrayList<String> authIntersectionSet) {
		for(StatementPattern stmt : stmtToLstAuthorities.keySet()) {
			Map<StatementSource, ArrayList<String>> stmtSourceToLstAuthorities = stmtToLstAuthorities.get(stmt);
			for(StatementSource src : stmtSourceToLstAuthorities.keySet()) {
				ArrayList<String> srcAuthSet = stmtSourceToLstAuthorities.get(src);
				srcAuthSet.retainAll(authIntersectionSet);
				if(srcAuthSet.size() == 0) {
					// System.out.println("removing " + src + " from " + stmt);
					List<StatementSource> sources = stmtToSources.get(stmt);
					synchronized (sources) {
						sources.remove(src);
					}
				}
			}
		}
	}

	/**
	 *  Get matching Subject authorities from a specific source for a triple pattern 
	 * @param stmt Triple pattern
	 * @param src Capable source 
	 * @return List of authorities
	 * @throws RepositoryException Repository Exception
	 * @throws MalformedQueryException Memory Exception
	 * @throws QueryEvaluationException Query Exception
	 */
	public ArrayList<String> FedSumD_getMatchingSbjAuthorities(StatementPattern stmt, StatementSource src) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		// System.out.println("endpointID: " + src.getEndpointID());
		String endPointUrl = src.getEndpointID().replace("sparql_", "http://");
		       endPointUrl = endPointUrl.replace("_", "/");
			   endPointUrl = endPointUrl.replace("=", "=http://");
		// System.out.println("endPointUrl: " + endPointUrl);
		ArrayList<String> sbjAuthorities = new ArrayList<String>();
		
		String queryString = getFedSumSbjAuthLookupQuery(stmt, endPointUrl);
		// System.out.println(queryString);
		TupleQuery tupleQuery = getSummaryConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		while(result.hasNext()) {
			BindingSet binding = result.next();
			// System.out.println("binding: " + binding);
			sbjAuthorities.add(binding.getValue("sbjAuth").stringValue());
		}
		return sbjAuthorities;
	}
	
	/**
	 *  A SPARQL query to retrieve matching subject authorities for a capable source of a triple pattern
	 * @param stmt Triple Pattern
	 * @param endPointUrl Url of the data source
	 * @return SPARQL query
	 */
	public String getFedSumSbjAuthLookupQuery(StatementPattern stmt,String endPointUrl) {
		String queryString = null;
		String s, p, o, sa = "null", oa = "null";

		if (stmt.getSubjectVar().getValue() != null) {
			s = stmt.getSubjectVar().getValue().stringValue();
			String[] sbjPrts = s.split("/");
			sa = sbjPrts[0] + "//" + sbjPrts[2];
		} else {
			s = "null";
		}
		    	
		if (stmt.getPredicateVar().getValue() != null) {
			p = stmt.getPredicateVar().getValue().stringValue();
		} else {
			p = "null";
		}
			
		if (stmt.getObjectVar().getValue() != null) {
			o = stmt.getObjectVar().getValue().stringValue();
			String[] objPrts = o.split("/");
			if ((objPrts.length > 1)) {
				oa = objPrts[0] + "//" + objPrts[2];
			} else {
				oa = "null";
			}    	 
		} else {
			o = "null"; 
		}
			
		if(!p.equals("null")) {
			queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
				+ "SELECT DISTINCT ?sbjAuth WHERE {"
				+ "    ?s ds:url <" + endPointUrl + ">."
				+ "    ?s ds:capability ?cap ."
				+ "    ?cap ds:predicate <" + p + "> ."
				+ "    ?cap ds:sbjAuthority  ?sbjAuth ."
				+ "}" ;	
		} else {
			if(sa.equals("null") && oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?sbjAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl+"> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:sbjAuthority ?sbjAuth ."
					+ "}";
			} else if(!sa.equals("null") && !oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?sbjAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:sbjAuthority  ?sbjAuth ."
					+ "    ?cap ds:objAuthority <" + oa + "> ."
					+ "    FILTER REGEX (str(?sbjAuth),'" + sa + "')"
					+ "}" ;
			} else if(!sa.equals("null") && oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?sbjAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:sbjAuthority  ?sbjAuth ."
					+ "    FILTER REGEX (str(?sbjAuth),'" + sa + "')"
					+ "}";
			} else if(sa.equals("null") && !oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?sbjAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:sbjAuthority  ?sbjAuth ."
					+ "    ?cap ds:objAuthority <" + oa + "> ."
					+ "}";
			}
		}				
		return queryString;
	}

	/**
	 *  Get matching object predicate authorities from a specific source for a triple pattern 
	 * @param stmt Triple pattern
	 * @param src Capable source 
	 * @param v Vertex
	 * @return List of authorities
	 * @throws RepositoryException  Repository Error
	 * @throws MalformedQueryException Memory Error
	 * @throws QueryEvaluationException Execution Error
	 */
	public ArrayList<String> FedSumD_getMatchingObjAuthorities(StatementPattern stmt, StatementSource src, Vertex v) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
		// System.out.println("endpointID: " + src.getEndpointID());
		String endPointUrl = src.getEndpointID().replace("sparql_", "http://");
		       endPointUrl = endPointUrl.replace("_", "/");
			   endPointUrl = endPointUrl.replace("=", "=http://");
		// System.out.println("endPointUrl: " + endPointUrl);
		// String endPointUrl = "http://" + src.getEndpointID().replace("sparql_", "");
	    // endPointUrl = endPointUrl.replace("_", "/");
	    String p = null;
		ArrayList<String> objAuthorities = new ArrayList<String>();
		if (stmt.getPredicateVar().getValue() != null) {
			p = stmt.getPredicateVar().getValue().stringValue();
		} else {
			p = stmt.getPredicateVar().getName().toString(); 
		}
		String queryString = getFedSumObjAuthLookupQuery(stmt, endPointUrl, v);
	    TupleQuery tupleQuery = getSummaryConnection().prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result = tupleQuery.evaluate();
		if(p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
			while(result.hasNext()) {
				String o = result.next().getValue("objAuth").stringValue();
				String[] objPrts = o.split("/");
				objAuthorities.add(objPrts[0] + "//" + objPrts[2]);
			}
		} else {
			while(result.hasNext()) {
				objAuthorities.add(result.next().getValue("objAuth").stringValue());
			}
		}
		return objAuthorities;
	}

	/**
	 *  A SPARQL query to retrieve matching object authorities for a capable source of a triple pattern
	 * @param stmt Triple Pattern
	 * @param endPointUrl Url of the data source
	 * @param v Vertex
	 * @return SPARQL query
	 */
	public String getFedSumObjAuthLookupQuery(StatementPattern stmt, String endPointUrl, Vertex v) {
		String queryString = null;
		String s, p, o, sa = "null", oa = "null";

		if (stmt.getSubjectVar().getValue() != null) {
			s = stmt.getSubjectVar().getValue().stringValue();
			String[] sbjPrts = s.split("/");
			sa = sbjPrts[0] + "//" + sbjPrts[2];
		} else {
			s = "null";  
		}   	
		
		if (stmt.getPredicateVar().getValue() != null) {
			p = stmt.getPredicateVar().getValue().stringValue();
		} else {
			if(stmt.getPredicateVar().getName().equals(v.label)) {
				p = stmt.getPredicateVar().getName();
			} else {
				p = "null"; 
			}
		}
		
		if (stmt.getObjectVar().getValue() != null) {
			o = stmt.getObjectVar().getValue().stringValue();
			String[] objPrts = o.split("/");
			if ((objPrts.length > 1)) {
				oa = objPrts[0] + "//" + objPrts[2];
			} else {
				oa = "null";
			}    	
		} else {
			o = "null"; 
		}
		
		if(!p.equals("null") && !stmt.getPredicateVar().getName().equals(v.label)) {
		 	queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
				+ "SELECT DISTINCT ?objAuth WHERE {"
				+ "    ?s ds:url <" + endPointUrl + "> ."
				+ "    ?s ds:capability ?cap ."
				+ "    ?cap ds:predicate <" + p + "> ."
				+ "    ?cap ds:objAuthority  ?objAuth ."
				+ "}";	
		} else {
			if(sa.equals("null") && oa.equals("null")) {
				queryString = "PREFIX ds:<http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?objAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:objAuthority ?objAuth ."
					+ "}";
			} else if(!sa.equals("null") && !oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?objAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:objAuthority  ?objAuth ."
					+ "    ?cap ds:objAuthority <" + oa + "> ."
					+ "    FILTER REGEX (str(?objAuth),'" + oa + "')"
					+ "}";
			} else if(sa.equals("null") && !oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?objAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:objAuthority ?objAuth ."
					+ "    FILTER REGEX (str(?objAuth),'" + oa + "')"
					+ "}";
			} else if(!sa.equals("null") && oa.equals("null")) {
				queryString = "PREFIX ds: <http://aksw.org/fedsum/> "
					+ "SELECT DISTINCT ?objAuth WHERE {"
					+ "    ?s ds:url <" + endPointUrl + "> ."
					+ "    ?s ds:capability ?cap ."
					+ "    ?cap ds:objAuthority ?objAuth ."
					+ "    ?cap ds:sbjAuthority <" + sa + "> ."
					+ "}";
			}
		}					
		return queryString;
	}

	protected static class SourceSelectionExecutorWithLatch {

		/**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch.
		 * Results are added to the map of the source selection instance. Errors 
		 * are reported as {@link OptimizationException} instances.
		 * @param hibiscusSourceSelection Quetsal Source Selection
		 * @param tasks Set of SPARQL ASK tasks
		 * @param cache Cache
		 */
		public static void run(ControlledWorkerScheduler scheduler, HibiscusSourceSelection hibiscusSourceSelection, List<CheckTaskPair> tasks, Cache cache) {
			new SourceSelectionExecutorWithLatch(scheduler, hibiscusSourceSelection).executeRemoteSourceSelection(tasks, cache);
		}		

		private final HibiscusSourceSelection sourceSelection;
		private final ControlledWorkerScheduler scheduler;

		private SourceSelectionExecutorWithLatch(ControlledWorkerScheduler scheduler, HibiscusSourceSelection hibiscusSourceSelection) {
		    this.scheduler = scheduler;
			this.sourceSelection = hibiscusSourceSelection;
		}

		/**
		 * Execute the given list of tasks in parallel, and block the thread until
		 * all tasks are completed. Synchronization is achieved by means of a latch
		 * 
		 * @param tasks
		 */
		private void executeRemoteSourceSelection(List<CheckTaskPair> tasks, Cache cache) {
			if (tasks.isEmpty()) {
				return;
			}

			List<Exception> errors = new ArrayList<Exception>();
			List<Future<Void>> futures = new ArrayList<Future<Void>>();
			for (CheckTaskPair task : tasks) {
				futures.add(scheduler.schedule(new ParallelCheckTask(task.e, task.t, sourceSelection), QueryInfo.getPriority() + 1));
			}

			for (Future<Void> future : futures) {
				try {
					future.get();
				} catch (InterruptedException e) {
					log.debug("Error during source selection. Thread got interrupted.");
					break;
				} catch (Exception e) {
					errors.add(e);
				}	
			}

			if (!errors.isEmpty()) {
				log.error(errors.size() + " errors were reported:");
				for (Exception e : errors) {
					log.error(ExceptionUtil.getExceptionString("Error occured", e));
				}
				Exception ex = errors.get(0);
				errors.clear();
				if (ex instanceof OptimizationException) {
					throw (OptimizationException) ex;
				}
				throw new OptimizationException(ex.getMessage(), ex);
			}
		}
	}

	public class CheckTaskPair {
		public final Endpoint e;
		public final StatementPattern t;
		public CheckTaskPair(Endpoint e, StatementPattern t) {
			this.e = e;
			this.t = t;
		}		
	}

	/**
	 * Task for sending an ASK request to the endpoints (for source selection)
	 * 
	 * @author Andreas Schwarte
	 */
	protected static class ParallelCheckTask implements Callable<Void> {

		final Endpoint endpoint;
		final StatementPattern stmt;
		final HibiscusSourceSelection sourceSelection;

		public ParallelCheckTask(Endpoint endpoint, StatementPattern stmt, HibiscusSourceSelection sourceSelection) {
			this.endpoint = endpoint;
			this.stmt = stmt;
			this.sourceSelection = sourceSelection;
		}

		@Override
		public Void call() throws Exception {
			try {
				TripleSource t = endpoint.getTripleSource();
				RepositoryConnection conn = endpoint.getConn(); 

				boolean hasResults = t.hasStatements(stmt, conn, EmptyBindingSet.getInstance());

				CacheEntry entry = CacheUtils.createCacheEntry(endpoint, hasResults);
				sourceSelection.cache.updateEntry( new SubQuery(stmt), entry);

				if (hasResults) {
					sourceSelection.addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));
				}	

				return null;
			} catch (Exception e) {
				throw new OptimizationException("Error checking results for endpoint " + endpoint.getId() + ": " + e.getMessage(), e);
			}
		}
	}
}




