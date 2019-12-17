package org.dtim.odin.storage.resources.bdi;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.system.Txn;
import org.dtim.odin.storage.bdi.extraction.Namespaces;
import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.jena.JenaConnection;
import org.dtim.odin.storage.db.mongo.repositories.DataSourceRepository;
import org.dtim.odin.storage.db.mongo.repositories.IntegratedDataSourcesRepository;
import org.dtim.odin.storage.db.mongo.utils.UtilsMongo;
import org.dtim.odin.storage.util.BdiSQLiteUtils;
import org.dtim.odin.storage.util.ConfigManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by Kashif-Rabbani in June 2019
 */
public class SchemaIntegrationHelper {
    private static final Logger LOGGER = Logger.getLogger(SchemaIntegrationHelper.class.getName());

    IntegratedDataSourcesRepository integratedDSR = new IntegratedDataSourcesRepository();

    DataSourceRepository dataSourceR = new DataSourceRepository();

    GraphOperations graphO = new GraphOperations();
    public SchemaIntegrationHelper() {
    }

    void processAlignment(JSONObject objBody, String integratedIRI, Resource s, Resource p, String query, String[] checkIfQueryContainsResult, String integrationType) {
        graphO.runAQuery(graphO.sparqlQueryPrefixes + query).forEachRemaining(triple -> {
            //System.out.println(triple.get("o") + " oo " + triple.get("oo"));
            checkIfQueryContainsResult[0] = "Query Returned Result > 0";

            if (triple.get("o") != null && triple.get("oo") != null) {
                if (triple.get("o") == triple.get("oo")) {

                    checkIfQueryContainsResult[1] = "Alignments between " + triple.get("o").asResource().getLocalName() + " elements.";

                    // Classes p (source 1 Class) and s (source2 Class)
                    if (triple.get("o").asResource().getLocalName().equals("Class")) {
                        String sparqlClassAProperties = " SELECT DISTINCT ?p WHERE { GRAPH <" + integratedIRI + "> { ?p rdfs:domain <" + objBody.getAsString("p") + "> . } }";
                        String sparqlClassBProperties = " SELECT DISTINCT ?p WHERE { GRAPH <" + integratedIRI + "> { ?p rdfs:domain <" + objBody.getAsString("s") + "> . } }";

                        List<String> listPropertiesClassA = getSparqlQueryResult(integratedIRI, sparqlClassAProperties);
                        List<String> listPropertiesClassB = getSparqlQueryResult(integratedIRI, sparqlClassBProperties);

                        //System.out.println(String.join(",", listPropertiesClassA));
                        //System.out.println(String.join(",", listPropertiesClassB));

                        String sql = "INSERT INTO Class (classA,classB,countPropClassA,countPropClassB,listPropClassA,listPropClassB,actionType,classType,userProvidedName) VALUES (" +
                                "'" + objBody.getAsString("p") + "'" + "," +
                                "'" + objBody.getAsString("s") + "'" + "," +
                                "'" + listPropertiesClassA.size() + "'" + "," +
                                "'" + listPropertiesClassB.size() + "'" + "," +
                                "'" + String.join(",", listPropertiesClassA) + "'" + "," +
                                "'" + String.join(",", listPropertiesClassB) + "'" + "," +
                                "'" + objBody.getAsString("actionType") + "'" + "," +
                                "'" + objBody.getAsString("classType") + "'" + "," +
                                "'" + objBody.getAsString("userProvidedName") + "'" +
                                " );";
                        //System.out.println("Inserting into SQLite Table Class");
                        BdiSQLiteUtils.executeQuery(sql);
                    }

                    // Properties p (source 1 property) and s (source2 Property)
                    if (triple.get("o").asResource().getLocalName().equals("Property")) {
                        HashMap<String, String> propDomainRange = getPropertiesInfo(objBody, integratedIRI);
                        String sql = "INSERT INTO Property " +
                                "(PropertyA,PropertyB,DomainPropA,DomainPropB,RangePropA,RangePropB,AlignmentType,hasSameName,actionType) VALUES (" +
                                "'" + objBody.getAsString("p") + "'" + ',' +
                                "'" + objBody.getAsString("s") + "'" + ',' +
                                "'" + propDomainRange.get("pDomain") + "'" + ',' +
                                "'" + propDomainRange.get("sDomain") + "'" + ',' +
                                "'" + propDomainRange.get("pRange") + "'" + ',' +
                                "'" + propDomainRange.get("sRange") + "'" + ',' +
                                "'" + objBody.getAsString("mapping_type") + "'" + ',' +
                                "'" + propDomainRange.get("hasSameName") + "'" + ',' +
                                "'" + objBody.getAsString("actionType") + "'" +
                                " ); ";
                        //System.out.println("Inserting into SQLite Table Property");
                        BdiSQLiteUtils.executeQuery(sql);
                    }
                }
            }
        });


    }

    private HashMap<String, String> getPropertiesInfo(JSONObject objBody, String integratedIRI) {
        HashMap<String, String> propCharacteristics = new HashMap<String, String>();

        Dataset ds = JenaConnection.getInstance().getTDBDataset();

        Txn.executeWrite(ds, ()-> {
            Model graph = ds.getNamedModel(integratedIRI);
            OntModel ontModel = org.apache.jena.rdf.model.ModelFactory.createOntologyModel();
            ontModel.addSubModel(graph);

            //System.out.println("if Properties: -> Printing Domain and Range: ... ");
            //System.out.println(ontModel.getOntProperty(objBody.getAsString("s")).getLocalName());
            //System.out.println(ontModel.getOntProperty(objBody.getAsString("p")).getLocalName());

            propCharacteristics.put("sDomain", ontModel.getOntProperty(objBody.getAsString("s")).getDomain().toString());
            propCharacteristics.put("sRange", ontModel.getOntProperty(objBody.getAsString("s")).getRange().toString());
            propCharacteristics.put("pDomain", ontModel.getOntProperty(objBody.getAsString("p")).getDomain().toString());
            propCharacteristics.put("pRange", ontModel.getOntProperty(objBody.getAsString("p")).getRange().toString());

            if (ontModel.getOntProperty(objBody.getAsString("s")).getLocalName().equals(ontModel.getOntProperty(objBody.getAsString("p")).getLocalName())) {
                propCharacteristics.put("hasSameName", "TRUE");
            } else {
                propCharacteristics.put("hasSameName", "FALSE");
            }
        }) ;

        return propCharacteristics;
    }

    void addInfo(JSONObject dataSource1Info, JSONObject dataSource2Info, String integratedModelFileName, JSONObject vowlObj) {
        // Constructing JSON Response Object

        JSONObject integratedDataSourceObj = new JSONObject();
        JSONArray dataSourcesArray = new JSONArray();

        JSONObject ds1 = new JSONObject();
        ds1.put("dataSourceID", dataSource1Info.getAsString("dataSourceID"));
        ds1.put("dataSourceName", dataSource1Info.getAsString("name"));
        ds1.put("alignmentsIRI", dataSource1Info.getAsString("dataSourceID") + "-" + dataSource2Info.getAsString("dataSourceID"));

        JSONObject ds2 = new JSONObject();
        ds2.put("dataSourceID", dataSource2Info.getAsString("dataSourceID"));
        ds2.put("dataSourceName", dataSource2Info.getAsString("name"));
        ds2.put("alignmentsIRI", dataSource1Info.getAsString("dataSourceID") + "-" + dataSource2Info.getAsString("dataSourceID"));


        dataSourcesArray.add(ds1);
        dataSourcesArray.add(ds2);

        integratedDataSourceObj.put("dataSourceID", "INTEGRATED-" + dataSource1Info.getAsString("dataSourceID") + "-" + dataSource2Info.getAsString("dataSourceID"));
        //integratedDataSourceObj.put("alignmentsIRI", alignmentsIRI.split(Namespaces.Alignments.val())[1]);
        integratedDataSourceObj.put("schema_iri", Namespaces.G.val() + dataSource1Info.getAsString("dataSourceID") + "-" + dataSource2Info.getAsString("dataSourceID"));
        integratedDataSourceObj.put("dataSources", dataSourcesArray);
        integratedDataSourceObj.put("name", dataSource1Info.getAsString("name").replaceAll(" ", "") + dataSource2Info.getAsString("name").replaceAll(" ", ""));
        integratedDataSourceObj.put("parsedFileAddress", integratedModelFileName);
        integratedDataSourceObj.put("graphicalGraph", "\" " + StringEscapeUtils.escapeJava(vowlObj.getAsString("vowlJson")) + "\"");
        //integratedDataSourceObj.put("integratedVowlJsonFilePath", vowlObj.getAsString("vowlJsonFilePath"));

        // Adding JSON Response in MongoDB Collection named as IntegratedDataSources
        addIntegratedDataSourceInfoAsMongoCollection(integratedDataSourceObj);
    }

    void updateInfo(JSONObject integratedDataSourceInfo, JSONObject dataSource2Info, String integratedModelFileName, JSONObject vowlObj) {
        JSONArray dataSourcesArray = (JSONArray) JSONValue.parse(integratedDataSourceInfo.getAsString("dataSources"));
        // New Local Graph data source to be integrated
        JSONObject ds2 = new JSONObject();
        ds2.put("dataSourceID", dataSource2Info.getAsString("dataSourceID"));
        ds2.put("dataSourceName", dataSource2Info.getAsString("name"));
        ds2.put("alignmentsIRI", integratedDataSourceInfo.getAsString("dataSourceID") + "-" + dataSource2Info.getAsString("dataSourceID"));

        dataSourcesArray.add(ds2);

        //System.out.println("Mongo Collection About to Upadte: ");
        String newDataSourceID = integratedDataSourceInfo.getAsString("dataSourceID") + "-" + dataSource2Info.getAsString("dataSourceID");

        //System.out.println("OLD DS ID: " + integratedDataSourceInfo.getAsString("dataSourceID"));
        //System.out.println("NEW DS ID: " + newDataSourceID);

        integratedDSR.updateByDataSourceID(integratedDataSourceInfo.getAsString("dataSourceID"),"dataSourceID", newDataSourceID);
        integratedDSR.updateByDataSourceID(newDataSourceID,"schema_iri", Namespaces.G.val() + integratedDataSourceInfo.getAsString("dataSourceID") + "-" + dataSource2Info.getAsString("dataSourceID"));
        integratedDSR.updateByDataSourceID(newDataSourceID,"graphicalGraph", "\" " + StringEscapeUtils.escapeJava(vowlObj.getAsString("vowlJson")) + "\"");
        integratedDSR.updateByDataSourceID(newDataSourceID,"parsedFileAddress", integratedModelFileName);
        integratedDSR.updateByDataSourceID(newDataSourceID,"dataSources", dataSourcesArray);
        integratedDSR.updateByDataSourceID(newDataSourceID,"name", integratedDataSourceInfo.getAsString("name").replaceAll(" ", "") + dataSource2Info.getAsString("name").replaceAll(" ", ""));
    }

    String integrateTDBDatasets(JSONObject dataSource1Info, JSONObject dataSource2Info) {

        String integratedIRI = Namespaces.G.val()
                + dataSource1Info.getAsString("dataSourceID") + "-"
                + dataSource2Info.getAsString("dataSourceID");
        LOGGER.info("Integrated IRI :" + integratedIRI);
        Dataset ds = JenaConnection.getInstance().getTDBDataset();

        Txn.executeWrite(ds, ()-> {
            Model ds1Model = ds.getNamedModel(dataSource1Info.getAsString("schema_iri"));
            Model ds2Model = ds.getNamedModel(dataSource2Info.getAsString("schema_iri"));
            LOGGER.info("Size of ds1 Model: " + ds1Model.size());
            LOGGER.info("Size of ds2 Model: " + ds2Model.size());

            Model integratedModel = ds1Model.union(ds2Model);

            graphO.loadModel(integratedIRI,integratedModel);
            LOGGER.info("Size of Integrated Model: " + integratedModel.size());

            try {
                integratedModel.write(new FileOutputStream("Output/integrated-model.ttl"), "TURTLE");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }) ;

        return integratedIRI;
    }

    public String getDataSourceInfo(String dataSourceId) {
        return UtilsMongo.ToJsonString(dataSourceR.findByDataSourceID(dataSourceId));
    }

    public String getIntegratedDataSourceInfo(String integratedDataSourceId) {
        return UtilsMongo.ToJsonString(integratedDSR.findByDataSourceID(integratedDataSourceId));
    }

    private void addIntegratedDataSourceInfoAsMongoCollection(JSONObject objBody) {
        LOGGER.info("Successfully Added to MongoDB");
        integratedDSR.create(objBody.toJSONString());
    }


    public void deleteDataSourceInfo(String dataSourceID, String collectionType) {
        if (collectionType.equals("INTEGRATED")) {
            integratedDSR.deleteByDataSourceID(dataSourceID);
        }
        if (collectionType.equals("DATA-SOURCE")) {
            dataSourceR.deleteByDataSourceID(dataSourceID);
        }
    }

    public List<String> getSparqlQueryResult(String namedGraph, String query) {
        List<String> temp = new ArrayList<>();
        graphO.runAQuery(graphO.sparqlQueryPrefixes + query).forEachRemaining(triple -> {
            temp.add(triple.get("p").toString());
        });
        return temp;
    }

    public void populateResponseArray(JSONArray tempAlignmentsArray, QuerySolution triple, JSONObject alignments) {
        alignments.put("s", triple.get("s").toString());
        alignments.put("p", triple.get("p").toString());
        alignments.put("confidence", triple.get("o").toString().split("__")[0]);
        alignments.put("mapping_type", triple.get("o").toString().split("__")[1]);
        alignments.put("lexical_confidence", triple.get("o").toString().split("__")[2]);
        alignments.put("structural_confidence", triple.get("o").toString().split("__")[3]);
        alignments.put("mapping_direction", triple.get("o").toString().split("__")[4]);
        tempAlignmentsArray.add(alignments);
    }

    public String writeToFile(String iri, String integratedIRI) {
        String integratedModelFileName = iri + ".ttl";
        // Write the integrated Graph into file by reading from TDB
        Dataset ds = JenaConnection.getInstance().getTDBDataset();
        Txn.executeWrite(ds, ()-> {
            Model model = ds.getNamedModel(integratedIRI);
            //System.out.println("iri: " + iri);

            //String integratedModelFileName = objBody.getAsString("dataSource1Name") + "-" + objBody.getAsString("dataSource2Name") + ".ttl";
            try {
                model.write(new FileOutputStream(ConfigManager.getProperty("output_path") + integratedModelFileName), "TURTLE");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        });
        return integratedModelFileName;
    }

    public static List<String> getPropertyTableFeatures() {
        List<String> propertyTableAttributes = new ArrayList<>();
        propertyTableAttributes.add("PropertyA");
        propertyTableAttributes.add("PropertyB");
        propertyTableAttributes.add("DomainPropA");
        propertyTableAttributes.add("DomainPropB");
        propertyTableAttributes.add("RangePropA");
        propertyTableAttributes.add("RangePropB");
        propertyTableAttributes.add("AlignmentType");
        propertyTableAttributes.add("hasSameName");
        propertyTableAttributes.add("actionType");
        return propertyTableAttributes;
    }

    public static List<String> getClassTableFeatures() {
        List<String> classTableAttributes = new ArrayList<>();
        classTableAttributes.add("classA");
        classTableAttributes.add("classB");
        classTableAttributes.add("countPropClassA");
        classTableAttributes.add("countPropClassB");
        classTableAttributes.add("listPropClassA");
        classTableAttributes.add("listPropClassB");
        classTableAttributes.add("actionType");
        classTableAttributes.add("classType");
        classTableAttributes.add("userProvidedName");
        return classTableAttributes;
    }

    public void initAlignmentTables() {
        BdiSQLiteUtils.createTable("Property", getPropertyTableFeatures());
        BdiSQLiteUtils.createTable("Class", getClassTableFeatures());
    }

    public static String calculateFileSize(String fileAddress) {
        File file = new File(fileAddress);

        if (file.exists()) {

            double bytes = file.length();
            double kilobytes = (bytes / 1024);
            double megabytes = (kilobytes / 1024);
            double gigabytes = (megabytes / 1024);
            double terabytes = (gigabytes / 1024);
            double petabytes = (terabytes / 1024);
            double exabytes = (petabytes / 1024);
            double zettabytes = (exabytes / 1024);
            double yottabytes = (zettabytes / 1024);

            System.out.println("bytes : " + bytes);
            System.out.println("kilobytes : " + kilobytes);
            System.out.println("megabytes : " + megabytes);
            System.out.println("gigabytes : " + gigabytes);
            System.out.println("terabytes : " + terabytes);
            System.out.println("petabytes : " + petabytes);
            System.out.println("exabytes : " + exabytes);
            System.out.println("zettabytes : " + zettabytes);
            System.out.println("yottabytes : " + yottabytes);
            return Double.toString(kilobytes);
        } else {
            System.out.println("File does not exists!");
            return "";
        }
    }
}
