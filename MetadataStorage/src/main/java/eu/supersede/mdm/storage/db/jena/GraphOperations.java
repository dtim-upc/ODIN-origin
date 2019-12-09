package eu.supersede.mdm.storage.db.jena;

import eu.supersede.mdm.storage.db.jena.query.SelectQuery;
import eu.supersede.mdm.storage.model.Namespaces;
import eu.supersede.mdm.storage.model.metamodel.GlobalGraph;
import eu.supersede.mdm.storage.util.TempFiles;
import eu.supersede.mdm.storage.util.Tuple3;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.sparql.resultset.ResultSetMem;
import org.apache.jena.system.Txn;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.util.FileManager;
import org.apache.jena.vocabulary.RDF;
import org.semarglproject.vocab.OWL;
import org.semarglproject.vocab.RDFS;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class GraphOperations {

    SelectQuery selectQ = new SelectQuery();


    private Dataset ds;

    public  GraphOperations() {
        ds = JenaConnection.getInstance().getTDBDataset();
    }

    public JSONArray getFeaturesFromGraph(String namedGraph){
        JSONArray features = new JSONArray();

        Query SPARQL = selectQ.selectSubjectsFromGraph(namedGraph,Namespaces.rdf.val()+"type",GlobalGraph.FEATURE.val());

        runAQuery(SPARQL).forEachRemaining(t -> {
            features.add(t.get("s").asNode().getURI());
        });
        return features;
    }

    public ResultSet getObjectsFromGraphWhere(String namedGraph,String whereSubject,String wherePredicate){
        return runAQuery(selectQ.selectObjectsFromGraph(namedGraph,whereSubject,wherePredicate));
    }

    public JSONObject getFeaturesWithConceptFromGraph(String namedGraph){
        JSONObject featureConcept = new JSONObject();

        Query SPARQL = selectQ.selectSubjectAndFeatureFromGraph(namedGraph,GlobalGraph.HAS_FEATURE.val());

        runAQuery(SPARQL).forEachRemaining(t -> {
            featureConcept.put(t.get("o").asNode().getURI(), t.get("s").asNode().getURI());
        });
        return featureConcept;
    }

    public void loadTTL(String namedGraph, String contentTTL) {
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            graph.read(new ByteArrayInputStream(contentTTL.getBytes()), null, "TTL");
        });

    }

    public void removeTriple(String namedGraph, String s, String p, String o) {
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(new ResourceImpl(s), new PropertyImpl(p), new ResourceImpl(o));
        });
    }

    public void addTriple(String namedGraph, String s, String p, String o) {
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(new ResourceImpl(s), new PropertyImpl(p), new ResourceImpl(o));
        });
    }

    /**
     *
     * @param namedGraph
     * @param triple JSON with keys s,p,o
     */
    public void addTriple(String namedGraph,JSONObject triple){
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(new ResourceImpl(triple.getAsString("s")), new PropertyImpl(triple.getAsString("p")), new ResourceImpl(triple.getAsString("o")));
        });
    }

    /**
     *
     * @param namedGraph
     * @param triple should have keys s,p and o
     */
    public void addTriple(String namedGraph,QuerySolution triple){
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(triple.getResource("s"), new PropertyImpl(triple.get("p").toString()), triple.getResource("o"));
        });
    }

    public void addBatchOfTriples(String namedGraph, List<Tuple3<String, String, String>> triples) {
        //System.out.println("Adding triple: [namedGraph] "+namedGraph+", [s] "+s+", [p] "+p+", [o] "+o);
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            for (Tuple3<String, String, String> t : triples) {
                graph.add(new ResourceImpl(t._1), new PropertyImpl(t._2), new ResourceImpl(t._3));
            }
        });
    }

    public void deleteAllTriples(String namedGraph){
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            graph.removeAll();
        });

    }


    public void createGraphWithOntModel(String iri, String fileRDF){
        Txn.executeWrite(ds, ()-> {
            Model model = ds.getNamedModel(iri);
            OntModel ontModel = ModelFactory.createOntologyModel();
            model.add(FileManager.get().readModel(ontModel, fileRDF));
        });
    }


    public void createGraph(String iri, String filePath){
        Txn.executeWrite(ds, ()-> {
            Model model = ds.getNamedModel(iri);
            model.read(filePath);
        });
    }

    public void createGraph(String iri){
        Txn.executeWrite(ds, ()-> {
            Model model = ds.getNamedModel(iri);
        });
    }

    //rename to delete
    public void removeGraph(String iri){
        if(ds.containsNamedModel(iri)){
            Txn.executeWrite(ds, ()-> {
                ds.removeNamedModel(iri);
            });
        }
    }



    public void loadModel(String namedGraph, Model model){
        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(model);
        });
    }



    public String selectTriplesFromGraphAsXML(String iri){
        Query SPARQL = selectQ.selectTriplesFromGraph(iri,null,null,null);
        ResultSet rs = runAQuery(SPARQL);
        return ResultSetFormatter.asXMLString(rs);
    }

    public Boolean containsIRIAsSubject(String namedGraph, String subjectIRI){
        ResultSet rsS = runAQuery(selectQ.selectDistinctPAndO(namedGraph,subjectIRI));

        if(((ResultSetMem) rsS).size() == 0 )
            return false;
        return true;
    }

    public Boolean containsIRIAsObject(String namedGraph, String objectIRI){
        ResultSet rsS = runAQuery(selectQ.selectDistinctSAndP(namedGraph,objectIRI));

        if(((ResultSetMem) rsS).size() == 0 )
            return false;
        return true;
    }

    public Boolean containsTriples(String namedGraph, String sIRI, String pIRI, String oIRI){

        ResultSet rs = runAQuery(selectQ.selectCountTriples(namedGraph,sIRI,pIRI,oIRI));

        if(((ResultSetMem) rs).size() == 0)
            return false;
        return true;
    }

    public void addProperty(String namedGraph, String property, String[] domains, String range) {

        Txn.executeWrite(ds, ()->{
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(new ResourceImpl(property), RDF.type, RDF.Property);
            for (String domain : domains) {
                graph.add(new ResourceImpl(property), new PropertyImpl(RDFS.DOMAIN), new ResourceImpl(domain));
            }
            graph.add(new ResourceImpl(property), new PropertyImpl(RDFS.RANGE), new ResourceImpl(range));

        });
    }


    public void addPropertyDomain(String namedGraph, String property, String domain) {
        Txn.executeWrite(ds, ()->{
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(new ResourceImpl(property), new PropertyImpl(RDFS.DOMAIN), new ResourceImpl(domain));
        });
    }

    public void addProperty(String namedGraph, String property, String domain, String range) {
        Txn.executeWrite(ds, ()->{
            Model graph = ds.getNamedModel(namedGraph);
            graph.add(new ResourceImpl(property), RDF.type, RDF.Property);
            graph.add(new ResourceImpl(property), new PropertyImpl(RDFS.DOMAIN), new ResourceImpl(domain));
            graph.add(new ResourceImpl(property), new PropertyImpl(RDFS.RANGE), new ResourceImpl(range));
        });

    }

    public void addCustomTriple(String namedGraph, String s, String p, String o) {
        Txn.executeWrite(ds, ()->{
            Model graph = ds.getNamedModel(namedGraph);
            if (p.equals("EQUIVALENT_CLASS"))
                graph.add(new ResourceImpl(s), new PropertyImpl(OWL.EQUIVALENT_CLASS), new ResourceImpl(o));
            if (p.equals("SUB_CLASS_OF"))
                graph.add(new ResourceImpl(s), new PropertyImpl(RDFS.SUB_CLASS_OF), new ResourceImpl(o));
            if (p.equals("EQUIVALENT_PROPERTY"))
                graph.add(new ResourceImpl(s), new PropertyImpl(OWL.EQUIVALENT_PROPERTY), new ResourceImpl(o));
            if (p.equals("DOMAIN"))
                graph.add(new ResourceImpl(s), new PropertyImpl(RDFS.DOMAIN), new ResourceImpl(o));
            if (p.equals("RANGE"))
                graph.add(new ResourceImpl(s), new PropertyImpl(RDFS.RANGE), new ResourceImpl(o));
        });
    }

    public void addClassOrPropertyTriple(String namedGraph, String s, String p) {
        Txn.executeWrite(ds, ()->{
            Model graph = ds.getNamedModel(namedGraph);
            if (p.equals("CLASS"))
                graph.add(new ResourceImpl(s), RDF.type, new ResourceImpl(RDFS.CLASS));
            if (p.equals("PROPERTY"))
                graph.add(new ResourceImpl(s), RDF.type, RDF.Property);
        });
    }



    public String getRDFString(String namedGraph) {

        String results = Txn.calculateRead(ds, ()-> {
            String content = "";
            Model graph = ds.getNamedModel(namedGraph);
            // Output RDF
            String tempFileForO = TempFiles.getTempFile();
            try {
                graph.write(new FileOutputStream(tempFileForO), "RDF/XML-ABBREV");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            try {
                content = new String(java.nio.file.Files.readAllBytes(new java.io.File(tempFileForO).toPath()));
            } catch (IOException exc) {
                exc.printStackTrace();
            }
            return content ;
        }) ;
        return results;
    }


    //////////////////////////////////////
    //
    //          Delete operations
    //
    //////////////////////////////////////

    public void deleteTriples(String graphIRI,String subjectIRI, String predicateIRI, String objectIRI){
//        RDFUtil.runAnUpdateQuery("DELETE WHERE { GRAPH <" + graphIRI + ">" +
//                " {<"+subjectIRI+"> <"+predicateIRI+"> <"+objectIRI+">} }");
        Txn.executeWrite(ds, ()->{
            Model graph = ds.getNamedModel(graphIRI);
            graph.remove(new ResourceImpl(subjectIRI), new PropertyImpl(predicateIRI), new ResourceImpl(objectIRI));
        });
    }

    /**
     *
     * @param graphIRI
     * @param triple with keys s, p and o
     */
    public void deleteTriples(String graphIRI, JSONObject triple){
        deleteTriples(graphIRI,triple.getAsString("s"),triple.getAsString("p"),triple.getAsString("o"));
    }

    public void deleteTriplesWithSubject(String graphIRI, String subjectIRI){
        runAnUpdateQuery("DELETE WHERE { GRAPH <" + graphIRI + ">" +
                " {<"+subjectIRI+"> ?p ?o} }");
    }

    public void deleteTriplesWithObject(String graphIRI, String objectIRI){
        runAnUpdateQuery("DELETE WHERE { GRAPH <" + graphIRI + ">" +
                " {?s ?p <"+objectIRI+"> } }");
    }

    public void deleteTriplesWithProperty(String graphIRI, String predicateIRI){
        runAnUpdateQuery("DELETE WHERE { GRAPH <" + graphIRI + ">" +
                " {?s <"+predicateIRI+"> ?o} }");
    }

    //////////////////////////////////////
    //
    //        Queries executions.
    //
    //////////////////////////////////////


    public ResultSet runAQuery(Query query) {

        ResultSet resultSet = Txn.calculateRead(ds, ()-> {
            try(QueryExecution qExec = QueryExecutionFactory.create(query, ds)) {
                return ResultSetFactory.copyResults(qExec.execSelect()) ;
            }
        }) ;
        return resultSet;
    }

    public ResultSet runAQuery(String query) {

        return runAQuery(QueryFactory.create(query));
    }


    /**
     * Delete triple with oldIri and insert new triple with newIri in jena graph
     * @param graphIRI iri of the graph that needs to be updated.
     * @param oldIRI actual iri that appears in the triples.
     * @param newIRI new iri that is going to replace the actual iri.
     */
    public void updateResourceNodeIRI(String graphIRI, String oldIRI, String newIRI){
        // Look and update triples where oldIRI is object.
        runAnUpdateQuery("WITH <"+graphIRI+">  DELETE {?s ?p <"+oldIRI+">} " +
                "INSERT {?s ?p <"+newIRI+">} WHERE {  ?s ?p <"+oldIRI+"> }");
        // Look and update triples where oldIRI is subject.
        runAnUpdateQuery("WITH <"+graphIRI+">  DELETE {<"+oldIRI+"> ?p ?o} " +
                "INSERT {<"+newIRI+"> ?p ?o} WHERE {  <"+oldIRI+"> ?p ?o }");
    }

    public  void runAnUpdateQuery(String sparqlQuery) {

        Txn.executeWrite(ds, ()->{

            try {
                UpdateAction.parseExecute(sparqlQuery, ds);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }



    /**
     * Below are the new methods, variables and declarations coming from BDI Project
     */

    public static String sparqlQueryPrefixes = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + "PREFIX G: <http://www.essi.upc.edu/~snadal/BDIOntology/Global/> \n "+ "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";


    public ResultSet runAQuery(String query, Model model) {

        try (QueryExecution qExec = QueryExecutionFactory.create(QueryFactory.create(query), model)) {
            ResultSetRewindable results = ResultSetFactory.copyResults(qExec.execSelect());
            qExec.close();
            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ResultSet runAQuery(String sparqlQuery, Dataset ds) {
        try (QueryExecution qExec = QueryExecutionFactory.create(QueryFactory.create(sparqlQuery), ds)) {
            ResultSetRewindable results = ResultSetFactory.copyResults(qExec.execSelect());
            qExec.close();
            return results;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    //Methods to change to other class
    // Short name
    public String nn(String s) {
        return s.replace(Namespaces.G.val(), "")
                .replace(Namespaces.S.val(), "")
                .replace(Namespaces.sup.val(), "")
                .replace(Namespaces.rdfs.val(), "")
                .replace(Namespaces.owl.val(), "")
                .replace(Namespaces.serginf.val(), "");
    }

    public String convertToURI(String name) {
        //If it is a semantic annotation, add the right URI
        if (name.equals("hasFeature")) {
            return GlobalGraph.HAS_FEATURE.val();
        } else if (name.equals("subClass") || name.equals("subClassOf")) {
            return Namespaces.rdfs.val() + "subClassOf";
        } else if (name.equals("ID") || name.equals("identifier")) {
            return Namespaces.sc.val() + "identifier";
        }

        //Otherwise, just add the SUPERSEDE one
        return Namespaces.sup.val() + name;
    }
}
