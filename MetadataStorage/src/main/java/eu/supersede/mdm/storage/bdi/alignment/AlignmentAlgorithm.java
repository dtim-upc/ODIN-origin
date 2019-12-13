package eu.supersede.mdm.storage.bdi.alignment;

import eu.supersede.mdm.storage.bdi.extraction.Namespaces;
import eu.supersede.mdm.storage.db.jena.GraphOperations;
import eu.supersede.mdm.storage.resources.bdi.SchemaIntegrationHelper;
import eu.supersede.mdm.storage.util.SQLiteUtils;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.openrdf.model.vocabulary.RDFS;

import javax.inject.Inject;
import java.util.HashMap;

/**
 * Created by Kashif-Rabbani in June 2019
 */
public class AlignmentAlgorithm {

    GraphOperations graphO = new GraphOperations();

    private JSONObject basicInfo;

    public AlignmentAlgorithm(JSONObject obj) {
        this.basicInfo = obj;
        alignClasses();
        alignProperties();
    }

    private void alignProperties() {
        JSONArray propertiesData = SQLiteUtils.executeSelect("SELECT * FROM Property", SchemaIntegrationHelper.getPropertyTableFeatures());
        propertiesData.forEach(node -> {
            HashMap<String, String> data = new HashMap<>();

            Object[] row = ((JSONArray) node).toArray();
            for (Object element : row) {
                JSONObject obj = (JSONObject) element;
                data.put(obj.getAsString("feature"), obj.getAsString("value"));
            }

            switch (data.get("actionType")) {
                case "ACCEPTED":
                    switch (data.get("AlignmentType")) {
                        case "OBJECT-PROPERTY":
                            //System.out.println("OBJECT-PROPERTY");
                            //TODO Handle the Object Property
                            //graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), "EQUIVALENT_PROPERTY", data.get("PropertyB"));
                            break;
                        case "DATA-PROPERTY":
                            //System.out.println("DATA-PROPERTY");
                            String query = "SELECT * FROM Class WHERE classA = '" + data.get("DomainPropA") + "' and classB = '" + data.get("DomainPropB") + "'";
                            //System.out.println(query);
                            JSONArray result = SQLiteUtils.executeSelect(query, SchemaIntegrationHelper.getClassTableFeatures());


                            //TODO Case 1 -  When classes of the properties are aligned
                            if (result.size() > 0) {

                                Object[] rowResult = ((JSONArray) result.get(0)).toArray();
                                HashMap<String, String> sqliteRow = new HashMap<>();
                                for (Object element : rowResult) {
                                    JSONObject obj = (JSONObject) element;
                                    sqliteRow.put(obj.getAsString("feature"), obj.getAsString("value"));
                                }

                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), RDFS.DOMAIN.toString(), data.get("DomainPropA"));
                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyB"), RDFS.DOMAIN.toString(), data.get("DomainPropB"));

                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), RDFS.RANGE.toString(), data.get("RangePropA"));
                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyB"), RDFS.RANGE.toString(), data.get("RangePropB"));


                                //Move the Properties to the Parent class
                                String newGlobalProperty = basicInfo.getAsString("integratedIRI") + "/" + ResourceFactory.createResource(data.get("PropertyA")).getLocalName();
                                //String newPropertyDomain = basicInfo.getAsString("integratedIRI") + "/" + ResourceFactory.createResource(data.get("DomainPropA")).getLocalName(); //+ "_" + ResourceFactory.createResource(data.get("DomainPropB")).getLocalName();
                                String newPropertyDomain = basicInfo.getAsString("integratedIRI") + "/" + sqliteRow.get("userProvidedName"); //+ "_" + ResourceFactory.createResource(data.get("DomainPropB")).getLocalName();

                                graphO.addProperty(basicInfo.getAsString("integratedIRI"), newGlobalProperty, newPropertyDomain, data.get("RangePropA"));

                                // Handle SameAs
                                graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), "EQUIVALENT_PROPERTY", newGlobalProperty);
                                graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyB"), "EQUIVALENT_PROPERTY", newGlobalProperty);

                            } else if (data.get("PropertyB").contains(Namespaces.G.val())) { // PropertyB is the one with global IRI, coming from integrated global graph
                                // Add domain of PropertyA as domain of PropertyB
                                graphO.addPropertyDomain(basicInfo.getAsString("integratedIRI"), data.get("PropertyB"), data.get("DomainPropA"));

                                //Remove domain and Range of PropertyA
                                //RDFUtil.removeProperty(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), data.get("DomainPropA"), data.get("RangePropA"));
                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), RDFS.DOMAIN.toString(), data.get("DomainPropA"));
                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), RDFS.RANGE.toString(), data.get("RangePropA"));

                                //Create sameAs edge to the PropertyA from PropertyB
                                graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyB"), "EQUIVALENT_PROPERTY", data.get("PropertyA"));
                            } else {
                                //TODO Case 2 -  When classes of the properties are not aligned
                                String newGlobalGraphProperty = basicInfo.getAsString("integratedIRI") + "/" + ResourceFactory.createResource(data.get("PropertyA")).getLocalName();

                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), RDFS.DOMAIN.toString(), data.get("DomainPropA"));
                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyB"), RDFS.DOMAIN.toString(), data.get("DomainPropB"));

                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), RDFS.RANGE.toString(), data.get("RangePropA"));
                                graphO.removeTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyB"), RDFS.RANGE.toString(), data.get("RangePropB"));


                                String[] domainsForNewGlobalPropertyResource = {data.get("DomainPropA"), data.get("DomainPropB")};

                                graphO.addProperty(basicInfo.getAsString("integratedIRI"), newGlobalGraphProperty, domainsForNewGlobalPropertyResource, data.get("RangePropA"));

                                // Handle SameAs
                                graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyA"), "EQUIVALENT_PROPERTY", newGlobalGraphProperty);
                                graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), data.get("PropertyB"), "EQUIVALENT_PROPERTY", newGlobalGraphProperty);
                            }
                            break;
                    }
                    break;
                case "REJECTED":
                    break;
            }
        });
    }


    private void alignClasses() {
        JSONArray classesData = SQLiteUtils.executeSelect("SELECT * FROM Class", SchemaIntegrationHelper.getClassTableFeatures());
        classesData.forEach(node -> {
            Object[] row = ((JSONArray) node).toArray();
            HashMap<String, String> classRow = new HashMap<>();
            for (Object element : row) {
                JSONObject obj = (JSONObject) element;
                classRow.put(obj.getAsString("feature"), obj.getAsString("value"));
            }

            switch (classRow.get("actionType")) {
                case "ACCEPTED":
                    switch (classRow.get("classType")) {
                        case "SUPERCLASS":
                            graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), classRow.get("classA"), "SUB_CLASS_OF", classRow.get("classB"));
                            break;
                        case "LOCALCLASS":
                            Resource classA = ResourceFactory.createResource(classRow.get("classA"));
                            //Resource classB = ResourceFactory.createResource(classRow.get("classB"));

                            //if (basicInfo.getAsString("integrationType").equals("LOCAL-vs-LOCAL")) {
                            //newGlobalGraphClassResource = integratedIRI + "/" + classA.getURI().split(Namespaces.Schema.val())[1];
                            String newGlobalGraphClassResource = basicInfo.getAsString("integratedIRI") + "/" + classRow.get("userProvidedName");
                            // }

                            //System.out.println("GG Resource: " + newGlobalGraphClassResource);

                            graphO.addClassOrPropertyTriple(basicInfo.getAsString("integratedIRI"), newGlobalGraphClassResource, "CLASS");
                            graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), classRow.get("classA"), "SUB_CLASS_OF", newGlobalGraphClassResource);
                            graphO.addCustomTriple(basicInfo.getAsString("integratedIRI"), classRow.get("classB"), "SUB_CLASS_OF", newGlobalGraphClassResource);
                            break;
                    }
                    break;
                case "REJECTED":
                    //TODO:
                    break;
            }


        });
    }
}
