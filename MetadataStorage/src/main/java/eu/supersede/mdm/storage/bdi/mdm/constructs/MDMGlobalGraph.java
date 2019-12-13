package eu.supersede.mdm.storage.bdi.mdm.constructs;

import eu.supersede.mdm.storage.db.jena.GraphOperations;
import eu.supersede.mdm.storage.db.mongo.models.GlobalGraphModel;
import eu.supersede.mdm.storage.db.mongo.repositories.GlobalGraphRepository;
import eu.supersede.mdm.storage.model.Namespaces;
import eu.supersede.mdm.storage.model.graph.RelationshipEdge;
import eu.supersede.mdm.storage.model.metamodel.GlobalGraph;
import eu.supersede.mdm.storage.parsers.OWLtoWebVOWL;
import eu.supersede.mdm.storage.util.Utils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;

import org.semarglproject.vocab.RDF;

import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.SimpleDirectedGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Kashif-Rabbani in June 2019, updated in September 2019
 */
public class MDMGlobalGraph {

    GlobalGraphRepository globalGraphR = new GlobalGraphRepository();

    GraphOperations graphO = new GraphOperations();

    private String bdiGgIri = "";
    private String mdmGgIri = "";
    private String bdiGgName = "";
    private String mdmGgGraphicalGraph = "";
    HashMap<String, String> nodesIds;

    public HashMap<String, String> getNodesIds() {
        return nodesIds;
    }

    MDMGlobalGraph(String name, String iri, String mdmGgIri) {
        this.bdiGgIri = iri;
        this.bdiGgName = name;
        this.mdmGgIri = mdmGgIri;
        run();
    }

    private void run() {
        constructGlobalGraph();
        getGraphicalVowlGraph();
        insertMdmGgInMongo();
    }

    private void getGraphicalVowlGraph() {
        try {
            OWLtoWebVOWL owltoWebVowl = new OWLtoWebVOWL();
            owltoWebVowl.setNamespace(Namespaces.G.val());
            owltoWebVowl.setTitle(bdiGgName);
            String vowlJson = owltoWebVowl.convert(mdmGgIri);
            nodesIds = owltoWebVowl.getNodesId();
            mdmGgGraphicalGraph = "\" " + StringEscapeUtils.escapeJava(vowlJson) + "\"";
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertMdmGgInMongo() {
        GlobalGraphModel globalGraph = new GlobalGraphModel();
        globalGraph.setGlobalGraphID(UUID.randomUUID().toString().replace("-", ""));
        globalGraph.setNamedGraph(mdmGgIri);
        globalGraph.setDefaultNamespace(Namespaces.G.val());
        globalGraph.setName(bdiGgName);
        globalGraph.setGraphicalGraph(mdmGgGraphicalGraph);

        globalGraphR.create(globalGraph);
    }

    /**
     * This method is to transform the Integrated Global Graph into MDM Global Graph i.e. Concepts, features etc...
     */
    private void constructGlobalGraph() {
        Dataset ds = Utils.getTDBDataset();
        ds.begin(ReadWrite.WRITE);

        //Create MDM Global Graph i.e. create a named graph in the TDB
        graphO.removeGraph(mdmGgIri);

        graphO.createGraph(mdmGgIri);
//        Model mdmGlobalGraph = ds.getNamedModel(mdmGgIri);
        //System.out.println("Size: " + mdmGlobalGraph.size());

        /* Query to get Classes from BDI Global Graph and convert to Concepts of MDM's Global Graph*/
        classesToConcepts(mdmGgIri);
        /* Query to get Properties from BDI Global Graph and convert to Features of MDM's Global Graph*/
        propertiesToFeatures(mdmGgIri);
        /* Query to get Object Properties from BDI Global Graph and convert to ????  of MDM's Global Graph AND Create hasRelation edge*/
        objectPropertiesToRelations(mdmGgIri);
        /* Query to get Classes and their properties from BDI Global Graph to create hasFeature edges between Concepts and Features of MDM Global Graph*/
        connectConceptsAndFeatures(mdmGgIri);
        //Query to get the sameAs or equivalentProperty relationship of features
        //handleSameAsEdges(mdmGgIri);
        //Query to connect classes having subClassOf relationships
        connectSuperAndSubClasses(mdmGgIri);

        //Sergi added
        //This is to create artificial concepts grouping all equivalent features
        //processEquivalentProperties(mdmGlobalGraph);

    }

    private void classesToConcepts(String mdmGgIri) {
        String getClasses = "SELECT * WHERE { GRAPH <" + bdiGgIri + "> { ?s ?p ?o. ?s rdf:type rdfs:Class. FILTER NOT EXISTS {?o rdf:type ?x.} }}";
        graphO.runAQuery(graphO.sparqlQueryPrefixes + getClasses).forEachRemaining(triple -> {
            //System.out.print(triple.getResource("s") + "\t");
            //System.out.print(triple.get("p") + "\t");
            //System.out.print(triple.get("o") + "\n");
            graphO.addTriple(mdmGgIri,triple.get("s").toString(), RDF.TYPE, GlobalGraph.CONCEPT.val() );
        });
    }

    private void propertiesToFeatures(String mdmGgIri) {
        String getProperties = " SELECT * WHERE { GRAPH <" + bdiGgIri + "> { ?property rdfs:domain ?domain; rdfs:range ?range . FILTER NOT EXISTS {?range rdf:type rdfs:Class.}} }";
        graphO.runAQuery(graphO.sparqlQueryPrefixes + getProperties).forEachRemaining(triple -> {
            //System.out.print(triple.get("property") + "\t");
            //System.out.print(triple.get("domain") + "\t");
            //System.out.print(triple.get("range") + "\n");
            graphO.addTriple(mdmGgIri,triple.get("property").toString(), RDF.TYPE,GlobalGraph.FEATURE.val());
        });
        /*Properties without domain and range*/
        String getAloneProperties = " SELECT * WHERE { GRAPH <" + bdiGgIri + "> " +
                "{ ?property rdf:type rdf:Property . FILTER NOT EXISTS {?property rdfs:domain ?d ; rdfs:range ?r. }} }";
        graphO.runAQuery(graphO.sparqlQueryPrefixes + getAloneProperties).forEachRemaining(triple -> {
            //System.out.print(triple.get("property") + "\n");
            graphO.addTriple(mdmGgIri, triple.get("property").toString(),   RDF.TYPE, GlobalGraph.FEATURE.val());
//            mdmGlobalGraph.add(triple.getResource("property"), new PropertyImpl(RDF.TYPE), new ResourceImpl(GlobalGraph.FEATURE.val()));
        });
    }

    private void objectPropertiesToRelations(String mdmGgIri) {
        String getObjectProperties = " SELECT * WHERE { GRAPH <" + bdiGgIri + "> { ?property rdfs:domain ?domain; rdfs:range ?range . ?range rdf:type rdfs:Class.} }";
        graphO.runAQuery(graphO.sparqlQueryPrefixes + getObjectProperties).forEachRemaining(triple -> {
            //System.out.print(triple.get("property") + "\t");
            //System.out.print(triple.get("domain") + "\t");
            //System.out.print(triple.get("range") + "\n");
            graphO.addTriple(mdmGgIri, triple.get("domain").toString(), triple.get("property").toString(), triple.get("range").toString());
//            mdmGlobalGraph.add(triple.getResource("domain"), new PropertyImpl(triple.get("property").toString()), triple.getResource("range"));
        });
        //connectConcepts
    }

    private void connectConceptsAndFeatures(String mdmGgIri) {
        String getClasses = "SELECT * WHERE { GRAPH <" + bdiGgIri + "> { ?s ?p ?o. ?s rdf:type rdfs:Class. }}";

        graphO.runAQuery(graphO.sparqlQueryPrefixes + getClasses).forEachRemaining(classResourceIRI -> {
            //System.out.println();
            //System.out.println();
            //System.out.print(triple.get("s") + "\n");
            Resource classResource = classResourceIRI.getResource("s");
            String getClassProperties = " SELECT * WHERE { GRAPH <" + bdiGgIri
                    + "> { ?property rdfs:domain <" + classResourceIRI.get("s") + ">; rdfs:range ?range. FILTER NOT EXISTS {?range rdf:type rdfs:Class.}}} ";

            graphO.runAQuery(graphO.sparqlQueryPrefixes + getClassProperties).forEachRemaining(propertyResourceIRI -> {
                System.out.print(propertyResourceIRI.get("property") + "\t");

                String baseBDIOntologyIRI = eu.supersede.mdm.storage.bdi.extraction.Namespaces.BASE.val();

                System.out.println("Class IRI: " + classResource.toString());
                /* There are two types of class IRIs here:
                 * 1: http://www.BDIOntology.com/schema/Y
                 * 2: http://www.BDIOntology.com/global/ialjGpo5-DFmJjbSC/XY
                 * Key here is that last value before / is the class name*/

                String[] bits = classResource.toString().split("/");
                String lastOneIsClassName = bits[bits.length - 1];
                System.out.println("Class Name: " + lastOneIsClassName);
                System.out.println("Property: " + propertyResourceIRI.getResource("property").toString());


                String queryToGetEquivalentPropertiesFromPG = "SELECT * WHERE { GRAPH <" + bdiGgIri + "> { ?x  owl:equivalentProperty <" + propertyResourceIRI.get("property") + ">. } }";
                AtomicReference<Boolean> eqPropExistenceFlag = new AtomicReference<>(false);

                graphO.runAQuery(graphO.sparqlQueryPrefixes + queryToGetEquivalentPropertiesFromPG).forEachRemaining(eqPropResourceIRI -> {
                    //System.out.println(eqPropExistenceFlag.get().toString());
                    eqPropExistenceFlag.set(true);
                    //System.out.println(eqPropExistenceFlag.get().toString());

                    //System.out.println("Equivalent Property: " + eqPropResourceIRI.get("x").toString());
                    String eqPropClass = (eqPropResourceIRI.get("x").toString().split(baseBDIOntologyIRI)[1]).split("/")[1];
                    //System.out.println("Class of Equivalent Property: " + eqPropClass);

                    if (eqPropClass.equals(lastOneIsClassName)) {
                        //System.out.println("Done! - eqPropClass.equals(lastOneIsClassName)");
                        graphO.addTriple(mdmGgIri,classResource.toString(),GlobalGraph.HAS_FEATURE.val(),eqPropResourceIRI.getResource("x").toString());
                    }

                    if (propertyResourceIRI.getResource("property").toString().contains(eu.supersede.mdm.storage.bdi.extraction.Namespaces.G.val())) {
                        /*Get the class IRI from equivalent Property IRI*/
                        //System.out.println("Global property Case");

                        //check the domain of the global property
                        // If it is a global property and it has only one domain then I think i should skip doing the below step
                        // i) Get the domain of the global property
                        ArrayList domains = new ArrayList();
                        String domainOfGlobalProperty = " SELECT * WHERE { GRAPH <" + bdiGgIri + "> { <" + propertyResourceIRI.get("property") + "> rdfs:domain ?domain. } }";
                        graphO.runAQuery(graphO.sparqlQueryPrefixes + domainOfGlobalProperty).forEachRemaining(triple -> {
                            //System.out.print(triple.get("property") + "\t");
                            //System.out.println(triple.get("domain") + "\t");
                            domains.add(triple.get("domain"));
                            //System.out.print(triple.get("range") + "\n");
                            //mdmGlobalGraph.add(triple.getResource("property"), new PropertyImpl(RDF.TYPE), new ResourceImpl(GlobalGraph.FEATURE.val()));
                        });

                        if (domains.size() > 1) {
                            String eqPropClassIRI = eu.supersede.mdm.storage.bdi.extraction.Namespaces.Schema.val() + eqPropClass;
                            graphO.addTriple(mdmGgIri, eqPropClassIRI, GlobalGraph.HAS_FEATURE.val(), eqPropResourceIRI.get("x").toString());
                            graphO.removeTriple(mdmGgIri,propertyResourceIRI.get("property").toString(), RDF.TYPE, GlobalGraph.FEATURE.val() );
                        } else {
                            //System.out.println(domains.size() + " is the domain size");
                            graphO.addTriple(mdmGgIri,domains.get(0).toString(), GlobalGraph.HAS_FEATURE.val(), propertyResourceIRI.get("property").toString());
                            graphO.addTriple(mdmGgIri,eqPropResourceIRI.get("x").toString(),RDF.TYPE, GlobalGraph.FEATURE.val());
                            graphO.addTriple(mdmGgIri,propertyResourceIRI.get("property").toString(), GlobalGraph.SAME_AS.val(), eqPropResourceIRI.get("x").toString());
                        }


                    }
                });
                if (!propertyResourceIRI.getResource("property").toString().contains(eu.supersede.mdm.storage.bdi.extraction.Namespaces.G.val())) {
                    graphO.addTriple(mdmGgIri,classResource.toString(), GlobalGraph.HAS_FEATURE.val(), propertyResourceIRI.get("property").toString());
                }
            });
        });
    }

    private void handleSameAsEdges(String mdmGgIri) {
        //System.out.println(bdiGgIri);
        String getClasses = "SELECT * WHERE { GRAPH <" + bdiGgIri + "> { ?s  owl:equivalentProperty ?p } }";
        //System.out.println("Finding same as relationships");
        graphO.runAQuery(graphO.sparqlQueryPrefixes + getClasses).forEachRemaining(triple -> {

            Resource A = null;
            Resource B = null;

            if (triple.getResource("p").toString().contains(eu.supersede.mdm.storage.bdi.extraction.Namespaces.G.val())) {
                A = triple.getResource("p");
                B = triple.getResource("s");
            }

            if (triple.getResource("s").toString().contains(eu.supersede.mdm.storage.bdi.extraction.Namespaces.G.val())) {
                A = triple.getResource("s");
                B = triple.getResource("p");
            }
            //System.out.print(A + "\t" + B + "\n");
            //System.out.print(triple.get("s") + "\n");
            /*WARNING: By declaring sameAs property as a feature will result in lot of unconnected nodes in the visualization of global graph*/
            graphO.addTriple(mdmGgIri,B.toString(), RDF.TYPE, GlobalGraph.FEATURE.val());
            //graphO.addTriple();(triple.getResource("p"), OWL.sameAs, triple.getResource("s"));
            graphO.addTriple(mdmGgIri,A.toString(), GlobalGraph.SAME_AS.val(), B.toString());
        });
    }

    private void connectSuperAndSubClasses(String mdmGgIri) {
        String getClasses = "SELECT * WHERE { GRAPH <" + bdiGgIri + "> { ?s ?p ?o. ?s rdfs:subClassOf ?o. }}";
        graphO.runAQuery(graphO.sparqlQueryPrefixes + getClasses).forEachRemaining(triple -> {
            //System.out.print(triple.getResource("s") + "\t");
            //System.out.print(triple.get("p") + "\t" + triple.get("o")  + "\n");
            graphO.addTriple(mdmGgIri,triple);
        });
    }

    private void processEquivalentProperties(Model mdmGlobalGraph) {
        String getEquivProperties = "SELECT * WHERE { GRAPH <" + bdiGgIri + "> { ?s owl:equivalentProperty ?o }}";

        //Connect in a graph equivalent properties
        //each connected component will represent a concept
        Graph<String, RelationshipEdge> G = new SimpleDirectedGraph<>(RelationshipEdge.class);

        graphO.runAQuery(graphO.sparqlQueryPrefixes + getEquivProperties).forEachRemaining(triple -> {
            G.addVertex(triple.getResource("s").getURI());
            G.addVertex(triple.getResource("o").getURI());
            G.addEdge(triple.getResource("s").getURI(),triple.getResource("o").getURI(),
                    new RelationshipEdge(UUID.randomUUID().toString()));
//            mdmGlobalGraph.add(triple.getResource("s"), new PropertyImpl(triple.get("p").toString()), triple.getResource("o"));
        });
        ConnectivityInspector<String, RelationshipEdge> c = new ConnectivityInspector<>(G);
        c.connectedSets().forEach(s -> {
            String concept = UUID.randomUUID().toString();
            mdmGlobalGraph.add(new ResourceImpl(concept),new PropertyImpl(RDF.TYPE),  new ResourceImpl(GlobalGraph.CONCEPT.val()));
            s.forEach(f -> {
                mdmGlobalGraph.add(new ResourceImpl(concept),new PropertyImpl(GlobalGraph.HAS_FEATURE.val()),
                        new ResourceImpl(f));
            });
        });
    }

    private String getClassNameFromIRI(String IRI) {
        String global = eu.supersede.mdm.storage.bdi.extraction.Namespaces.G.val();
        String schema = eu.supersede.mdm.storage.bdi.extraction.Namespaces.Schema.val();
        String className = null;
        if (IRI.contains(global)) {
            className = IRI.split(global)[1].split("/")[0];
        }

        if (IRI.contains(schema)) {
            className = IRI.split(schema)[1].split("/")[0];
        }
        return className;
    }
}
