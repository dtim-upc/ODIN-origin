package org.dtim.odin.storage.parsers;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

import java.util.*;

public class ImportOWLtoGlobalGraph {

    GlobalGraphRepository globalGraphR = new GlobalGraphRepository();

    GraphOperations graphO = GraphOperations.getInstance();

    public void convert(String filePath, String name){

        Model model = ModelFactory.createDefaultModel() ;
        model.read(filePath, "RDF/XML") ;

        List<String> features = new ArrayList<>();
        Set<String> objectProperties = Sets.newHashSet();
        HashMap<String,String> allDomain = Maps.newHashMap();
        HashMap<String,String> allRange = Maps.newHashMap();

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

            //Object properties
            if (t.getPredicate().getURI().equals(Namespaces.rdf.val() + "type") &&
                    t.getObject().getURI().equals(Namespaces.owl.val()+"ObjectProperty")) {
                objectProperties.add(t.getSubject().getURI());
                globalGraphModel.add(new ResourceImpl(t.getSubject().getURI()),
                        new PropertyImpl( Namespaces.rdf.val() + "type"), new ResourceImpl(GlobalGraph.HAS_RELATION.val()));
            }

            //All domains
            if (t.getPredicate().getURI().equals(Namespaces.rdfs.val()+"domain"))
                allDomain.put(t.getSubject().getURI(),t.getObject().getURI());
            //All ranges
            if (t.getPredicate().getURI().equals(Namespaces.rdfs.val()+"range"))
                allRange.put(t.getSubject().getURI(),t.getObject().getURI());
        }

        //connect features with concepts
        triples.forEach(t -> {
            if(features.contains(t.getSubject().getURI())){
                globalGraphModel.add(new ResourceImpl(t.getObject().getURI()), new PropertyImpl( GlobalGraph.HAS_FEATURE.val()), new ResourceImpl(t.getSubject().getURI()));
            }
        });

        //process concept relationships
        objectProperties.forEach(op -> {
            if (allDomain.containsKey(op) && allRange.containsKey(op)) {
                globalGraphModel.add(new ResourceImpl(allDomain.get(op)),
                        new PropertyImpl( op ), new ResourceImpl(allRange.get(op)));
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
