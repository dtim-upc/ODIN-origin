package org.dtim.odin.storage.parsers;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.mongo.models.GlobalGraphModel;
import org.dtim.odin.storage.db.mongo.repositories.GlobalGraphRepository;
import org.dtim.odin.storage.model.Namespaces;
import org.dtim.odin.storage.model.metamodel.GlobalGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ImportOWLtoGlobalGraph {

    GlobalGraphRepository globalGraphR = new GlobalGraphRepository();

    GraphOperations graphO = GraphOperations.getInstance();

    public void convert(String filePath, String name){

        Model model = ModelFactory.createDefaultModel() ;
        model.read(filePath, "RDF/XML") ;

        List<String> features = new ArrayList<>();
        Model globalGraphModel = ModelFactory.createDefaultModel();
        String defaultNamespace = "";

        List<Triple> triples = new ArrayList<>();
        graphO.runAQuery("SELECT * WHERE {?s ?p ?o FILTER (!isBlank(?s) && !isBlank(?o)) }  ", model).forEachRemaining(triple -> {
            Triple t = new Triple(NodeFactory.createURI(triple.get("s").toString()),NodeFactory.createURI(triple.get("p").toString()),NodeFactory.createURI(triple.get("o").toString()));
            triples.add(t);
        });

        for(Triple t: triples) {
            //Concept
            if ( !t.getSubject().isBlank() && t.getPredicate().getURI().equals(Namespaces.rdf.val() + "type") &&
                    t.getObject().getURI().equals(Namespaces.owl.val()+"Class")
            ) {
                globalGraphModel.add(new ResourceImpl(t.getSubject().getURI()), new PropertyImpl( Namespaces.rdf.val() + "type"), new ResourceImpl(GlobalGraph.CONCEPT.val()));
            }

            //Get the base IRI
            if(t.getPredicate().getURI().equals(Namespaces.rdf.val() + "type") &&
                    t.getObject().getURI().equals(Namespaces.owl.val()+"Ontology")){
                defaultNamespace = t.getSubject().getURI().toString();
            }

            //SubClassOf
            if (t.getPredicate().getURI().equals(Namespaces.rdfs.val() + "subClassOf")) {
                globalGraphModel.add(new ResourceImpl(t.getSubject().getURI()), new PropertyImpl(Namespaces.rdfs.val() + "subClassOf"), new ResourceImpl(t.getObject().getURI()));
            }

            //Feature
            if (t.getPredicate().getURI().equals(Namespaces.rdf.val() + "type") &&
                    t.getObject().getURI().equals(Namespaces.owl.val()+"DatatypeProperty")
            ) {
                features.add(t.getSubject().getURI());
                globalGraphModel.add(new ResourceImpl(t.getSubject().getURI()), new PropertyImpl( Namespaces.rdf.val() + "type"), new ResourceImpl(GlobalGraph.FEATURE.val()));
            }
        }

        //connect features with concepts
        triples.forEach(t -> {
            if(features.contains(t.getSubject().getURI())){
                globalGraphModel.add(new ResourceImpl(t.getObject().getURI()), new PropertyImpl( GlobalGraph.HAS_FEATURE.val()), new ResourceImpl(t.getSubject().getURI()));
            }
        });

        defaultNamespace = defaultNamespace.charAt(defaultNamespace.length()-1) == '/' ?defaultNamespace : defaultNamespace + "/";
        String namedGraph = defaultNamespace+UUID.randomUUID().toString().replace("-","");
//        RDFUtil.loadModel(namedGraph,globalGraphModel);
        graphO.loadModel(namedGraph,globalGraphModel);
        saveInMongo(defaultNamespace,namedGraph,name);

    }

    public void saveInMongo(String defaultNamespace,String namedGraph, String name){

        GlobalGraphModel objGG = new GlobalGraphModel();
        objGG.setName(name);
        objGG.setGlobalGraphID(UUID.randomUUID().toString().replace("-",""));
        objGG.setDefaultNamespace(defaultNamespace);
        objGG.setNamedGraph(namedGraph);

        ImportOWLtoWebVowl graphicalConverter = new ImportOWLtoWebVowl();
        graphicalConverter.setNamespace(defaultNamespace);
        graphicalConverter.setTitle(name);
        String vowlJson = graphicalConverter.convert(namedGraph,defaultNamespace);
        String graphicalG = "\" " + StringEscapeUtils.escapeJava(vowlJson) + "\"";

        objGG.setGraphicalGraph(graphicalG);

        globalGraphR.create(objGG);

    }

}
