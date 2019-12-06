package eu.supersede.mdm.storage.db.jena.query;

import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;

public class SelectQuery {

    private Var VAR_graph = Var.alloc( "g" );
    private Var VAR_subject = Var.alloc( "s" );
    private Var VAR_predicate = Var.alloc( "p" );
    private Var VAR_object = Var.alloc( "o" );


    /**
     *
     * @return PRODUCES: SELECT * WHERE { GRAPH ?g {?s ?p ?o} }
     */
    private SelectBuilder getDefaultSelectAllFromGraph(){
        SelectBuilder query = new SelectBuilder().addGraph( VAR_graph,VAR_subject, VAR_predicate, VAR_object );
        return query;
    }

    //"SELECT ?s ?p ?o WHERE { GRAPH <" + wrapper + "> { ?s ?p ?o } }" CHECK THIS CASE !!!
    //"SELECT * WHERE { GRAPH <" + alignmentsIRI + "> {?s ?p ?o} }"

    /**
     *
     * PRODUCES when predicate and object null:
     *      SELECT * WHERE { GRAPH <namedGraph> {<subject> ?p ?o} } depending on parameters.
     *
     * PRODUCES when subject, predicate, object null:
     *      SELECT * WHERE { GRAPH <namedGraph> {?s ?p ?o} } depending on parameters.
     *
     * @param namedGraph
     * @param subject to set in the where clause. If null, a var ?s is used.
     * @param predicate to set in the where clause. If null, a var ?p is used.
     * @param object to set in the where clause. If null, a var ?o is used.
     * @return
     */
    public Query selectTriplesFromGraph(String namedGraph, String subject, String predicate, String object){
        SelectBuilder query = getDefaultSelectAllFromGraph();
        query.setVar( VAR_graph, NodeFactory.createURI( namedGraph ) ) ;
        if(subject != null)
            query.setVar( VAR_subject, NodeFactory.createURI( subject ) ) ;
        if(predicate != null)
            query.setVar( VAR_predicate, NodeFactory.createURI( predicate ) ) ;
        if(object != null)
            query.setVar( VAR_object, NodeFactory.createURI( object ) ) ;
        return query.build();
    }

    //Based on:
    //"SELECT ?g WHERE { GRAPH ?g { <" + c + "> <" + Namespaces.rdf.val() + "type" + "> <" + GlobalGraph.CONCEPT.val() + "> } }"
    //"SELECT ?g WHERE { GRAPH ?g { <" + c + "> <" + GlobalGraph.HAS_FEATURE.val() + "> <" + f + "> } }"
    //"SELECT ?g WHERE { GRAPH ?g { <" + s + "> <" + e + "> <" + t + "> } }"

    /**
     * CHECKED
     * Select graph ?g based on the where clause build.
     * @param subject to set in the where clause. If null, a var ?s is used.
     * @param predicate to set in the where clause. If null, a var ?p is used.
     * @param object to set in the where clause. If null, a var ?o is used.
     * @return PRODUCES:SELECT  ?g WHERE  { GRAPH ?g  { <subject>  <predicate>  <object>}}
     */
    public Query selectGraph(String subject, String predicate, String object){
        SelectBuilder query = getDefaultSelectAllFromGraph().addVar(VAR_graph);
        if(subject != null)
            query.setVar( VAR_subject, NodeFactory.createURI( subject ) ) ;
        if(predicate != null)
            query.setVar( VAR_predicate, NodeFactory.createURI( predicate ) ) ;
        if(object != null)
            query.setVar( VAR_object, NodeFactory.createURI( object ) ) ;
        return query.build();
    }

    //Based on: "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }"

    /**
     * CHECKED
     * @return PRODUCES: SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }
     */
    public Query selectAllGraphs(){
        SelectBuilder query = getDefaultSelectAllFromGraph().addVar(VAR_graph).setDistinct(true);
        return query.build();
    }

    //PRODUCES: SELECT ?o WHERE { ?s ?p ?o}
    //BASED ON:
    // "SELECT ?f WHERE {<"+c+"> <"+ GlobalGraph.HAS_FEATURE.val()+"> ?f }"
    public Query selectAllObjects(String subject, String predicate){
        SelectBuilder query = new SelectBuilder().addVar(VAR_object).addWhere(VAR_subject, VAR_predicate, VAR_object );
        if(subject != null)
            query.setVar( VAR_subject, NodeFactory.createURI( subject ) ) ;
        if(predicate != null)
            query.setVar( VAR_predicate, NodeFactory.createURI( predicate ) ) ;
        return query.build();
    }

    //BASED ON :SELECT ?f WHERE { GRAPH <"+namedGraph+"> { ?f <"+Namespaces.rdf.val()+"type> <"+GlobalGraph.FEATURE.val()+"> } }";
    /**
     * CHECKED
     * @param namedGraph
     * @param predicate to set in the where clause. If null, a var ?p is used.
     * @param object to set in the where clause. If null, a var ?o is used.
     * @return SELECT ?s WHERE {GRAPH <namedGraph>  { ?s ?p ?o } } in case predicate and object null
     */
    public Query selectSubjectsFromGraph(String namedGraph,  String predicate, String object){
        SelectBuilder query = getDefaultSelectAllFromGraph().addVar(VAR_subject);;
        query.setVar( VAR_graph, NodeFactory.createURI( namedGraph ) ) ;
        if(predicate != null)
            query.setVar( VAR_predicate, NodeFactory.createURI( predicate ) ) ;
        if(object != null)
            query.setVar( VAR_object, NodeFactory.createURI( object ) ) ;
        return query.build();
    }


    //"SELECT ?t WHERE { GRAPH <" + globalGraphIRI + "> { <" + sourceIRI + "> <" Namespaces.rdf.val() + "type> ?t } }"
    /**
     * CHECKED
     * @param namedGraph
     * @param subject to set in the where clause. If null, a var ?s is used.
     * @param predicate to set in the where clause. If null, a var ?p is used.
     * @return SELECT ?o WHERE {GRAPH <namedGraph>  { ?s ?p ?o } } in case predicate and subject null
     */
    public Query selectObjectsFromGraph(String namedGraph,  String subject, String predicate){
        SelectBuilder query = getDefaultSelectAllFromGraph().addVar(VAR_subject);;
        query.setVar( VAR_graph, NodeFactory.createURI( namedGraph ) ) ;
        if(predicate != null)
            query.setVar( VAR_predicate, NodeFactory.createURI( predicate ) ) ;
        if(subject != null)
            query.setVar( VAR_subject, NodeFactory.createURI( subject ) ) ;
        return query.build();
    }

    /**
     *
     * @param namedGraph
     * @param predicate to set in the where clause. If null, a var ?p is used.
     * @return SELECT ?s ?o WHERE {GRAPH <namedGraph>  { ?s ?p ?o } } in case predicate null
     */
    public Query selectSubjectAndFeatureFromGraph(String namedGraph,  String predicate){
        SelectBuilder query = getDefaultSelectAllFromGraph().addVar(VAR_subject).addVar(VAR_object);
        query.setVar( VAR_graph, NodeFactory.createURI( namedGraph ) ) ;
        if(predicate != null)
            query.setVar( VAR_predicate, NodeFactory.createURI( predicate ) ) ;
        return query.build();
    }

    public Query selectDistinctPAndO(String graphIRI, String subjectIRI){
        return QueryFactory.create("SELECT DISTINCT ?p ?o WHERE { GRAPH <" + graphIRI + "> {<"+subjectIRI+"> ?p ?o} }");
    }

    public Query selectDistinctSAndP(String graphIRI,String objectIRI){
        return QueryFactory.create("SELECT DISTINCT ?s ?p WHERE { GRAPH <" + graphIRI + "> {?s ?p <"+objectIRI+"> } }");
    }

    public Query selectCountTriples(String graphIRI, String subjectIRI, String predicateIRI, String objectIRI){
        return QueryFactory.create("SELECT (COUNT(*) AS ?count) WHERE { GRAPH <" + graphIRI + "> " +
                "{<"+subjectIRI+"> <"+predicateIRI+"> <"+objectIRI+">} }");
    }

    /////////////////////////////////
    //
    //  Delete queries
    //
    /////////////////////////////////


    public String delete(String graph, String subject, String predicate, String object){
        return  "DELETE WHERE { GRAPH <" + graph + "> {<"+subject+"> <"+predicate+"> <"+object+">} }";
    }


    /////////////////////////////////
    //
    //  Update queries
    //
    /////////////////////////////////








}
