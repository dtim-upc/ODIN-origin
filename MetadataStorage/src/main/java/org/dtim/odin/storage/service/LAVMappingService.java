package org.dtim.odin.storage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.result.DeleteResult;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.mongo.models.*;
import org.dtim.odin.storage.db.mongo.models.fields.LAVMappingMongo;
import org.dtim.odin.storage.db.mongo.repositories.DataSourceRepository;
import org.dtim.odin.storage.db.mongo.repositories.GlobalGraphRepository;
import org.dtim.odin.storage.db.mongo.repositories.LAVMappingRepository;
import org.dtim.odin.storage.db.mongo.repositories.WrapperRepository;
import org.dtim.odin.storage.model.Namespaces;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LAVMappingService {

    WrapperRepository wrapperR = new WrapperRepository();

    DataSourceRepository dataSourceR =new  DataSourceRepository();

    LAVMappingRepository LAVMappingR = new LAVMappingRepository();

    GlobalGraphRepository globalGR = new GlobalGraphRepository();

    GraphOperations graphO = new GraphOperations();

    //delete operations

    public void delete(String LAVMappingID){
        LAVMappingModel LAVMappingObject = LAVMappingR.findByLAVMappingID(LAVMappingID);
        WrapperModel wrapperObject = wrapperR.findByWrapperID(LAVMappingObject.getWrapperID());
        DataSourceModel dataSourceObject = dataSourceR.findByDataSourceID(wrapperObject.getDataSourceID());

        delete(LAVMappingObject,wrapperObject,dataSourceObject);
    }

    public void delete(LAVMappingModel LAVMappingObject, WrapperModel wrapperObject, DataSourceModel dataSourceObject){
        // Remove the sameAs edges
        for (LAVsameAs el : LAVMappingObject.getSameAs()) {
            String feature = el.getFeature();
            String attribute = el.getAttribute();
            graphO.deleteTriples(dataSourceObject.getIri(), attribute, Namespaces.owl.val() + "sameAs",feature);
        }

        //Remove the named graph of that mapping
        graphO.removeGraph(wrapperObject.getIri());

        //Remove the associated metadata from MongoDB
        removeLAVMappingFromMongo(LAVMappingObject.getLAVMappingID());
    }


    public void removeLAVMappingFromMongo(String LAVMappingID){
        DeleteResult result = LAVMappingR.deleteByLAVMappingID(LAVMappingID);
    }

    //create operations

    public JSONObject createLAVMappingMapsTo(String body){
        JSONObject objBody = (JSONObject) JSONValue.parse(body);

        WrapperModel wrapper = wrapperR.findByWrapperID(objBody.getAsString("wrapperID"));
        DataSourceModel dataSource =  dataSourceR.findByDataSourceID(wrapper.getDataSourceID());
        objBody = LAVMappingR.create(objBody);

        String dsIRI = dataSource.getIri();

        ((JSONArray) objBody.get("sameAs")).forEach(mapping -> {
            JSONObject objMapping = (JSONObject) mapping;
            graphO.addTriple(dsIRI,objMapping.getAsString("attribute"), Namespaces.owl.val() + "sameAs", objMapping.getAsString("feature"));
        });
        return objBody;

    }

    public JSONObject updateLAVMappingMapsTo(String body){
        JSONObject objBody = (JSONObject) JSONValue.parse(body);

        WrapperModel wrapper = wrapperR.findByWrapperID(objBody.getAsString("wrapperID"));
        DataSourceModel dataSource =  dataSourceR.findByDataSourceID(wrapper.getDataSourceID());

        updateTriples(((JSONArray) objBody.get("sameAs")), objBody.getAsString("LAVMappingID"),
                wrapper.getIri(), dataSource.getIri());

        return objBody;
    }

    public JSONObject createLAVMappingSubgraph(String body){
        JSONObject objBody = (JSONObject) JSONValue.parse(body);

        LAVMappingModel objMapping = LAVMappingR.findByLAVMappingID(objBody.getAsString("LAVMappingID"));

        List<String> list = new ArrayList<>();
        try {
            list = new ObjectMapper().readValue(objBody.getAsString("graphicalSubGraph"), List.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        LAVMappingR.updateGraphicalSubgraph(objMapping.getLAVMappingID().toString(),
               list);

        WrapperModel wrapper = wrapperR.findByWrapperID(objMapping.getWrapperID());
        GlobalGraphModel globalGraph = globalGR.findByGlobalGraphID(objMapping.getGlobalGraphID());

        String globalGraphIRI = globalGraph.getNamedGraph();
        String wIRI = wrapper.getIri();

        graphO.deleteAllTriples(wIRI);

        ((JSONArray) objBody.get("selection")).forEach(selectedElement -> {
            JSONObject objSelectedElement = (JSONObject) selectedElement;
            if (objSelectedElement.containsKey("target")) {
                String sourceIRI = ((JSONObject) objSelectedElement.get("source")).getAsString("iri");
                String relIRI = objSelectedElement.getAsString("name");
                String targetIRI = ((JSONObject) objSelectedElement.get("target")).getAsString("iri");

                graphO.addTriple(wIRI, sourceIRI, relIRI, targetIRI);

                //Extend to also incorporate the type of the added triple. This is obtained from the original global graph
                String typeOfSource = graphO.runAQuery("SELECT ?t WHERE { GRAPH <" + globalGraphIRI + "> { <" + sourceIRI + "> <"
                        + Namespaces.rdf.val() + "type> ?t } }").next().get("t").toString();

               String typeOfTarget = graphO.runAQuery("SELECT ?t WHERE { GRAPH <" + globalGraphIRI + "> { <" + targetIRI + "> <"
                       + Namespaces.rdf.val() + "type> ?t } }").next().get("t").toString();

                graphO.addTriple(wIRI,sourceIRI,Namespaces.rdf.val() + "type", typeOfSource);
                graphO.addTriple(wIRI, targetIRI, Namespaces.rdf.val() + "type", typeOfTarget);

                //Check if the target is an ID feature
                if (graphO.runAQuery("SELECT ?sc WHERE { GRAPH <" + globalGraphIRI + "> { <" + targetIRI + "> <" +
                        Namespaces.rdfs.val() + "subClassOf> <" + Namespaces.sc.val() + "identifier> } }").hasNext()) {
                    graphO.addTriple(wIRI, targetIRI, Namespaces.rdfs.val() + "subClassOf", Namespaces.sc.val() + "identifier");
                }

            }
        });

        return objBody;
    }


    //update operations

    /**
     * Updates feature iri in datasource, lavmapping and deletes triples from wrapper.
     * @param features array of modified features.
     * @param LAVMappingID id of the LAVMapping to be updated in mongodb.
     * @param wrapperIRI IRI of the wrapper to be deleted in jena.
     * @param datasourceIRI IRI of the datasource to be updated in jena.
     */
    public void updateTriples(JSONArray features,String LAVMappingID, String wrapperIRI, String datasourceIRI){

        for (Object selectedElement : features) {
            JSONObject objSelectedElement = (JSONObject) selectedElement;
            String oldIRI = objSelectedElement.getAsString("featureOld");
            String newIRI = objSelectedElement.getAsString("featureNew");

            updateLavMappingSameAsFeature(LAVMappingID,oldIRI,newIRI);
            graphO.updateResourceNodeIRI(datasourceIRI,oldIRI,newIRI);
        }
//        RDFUtil.deleteTriplesNamedGraph(wrapperIRI);
        graphO.deleteAllTriples(wrapperIRI);
        deleteGraphicalSubgraph(LAVMappingID);
    }

    public void deleteGraphicalSubgraph(String LAVMappingID){
        LAVMappingR.deleteField(LAVMappingID, LAVMappingMongo.FIELD_graphicalSubGraph.val());
    }

    /**
     * Updates the feature IRI from sameAs array of a LavMapping collection in MongoDB
     * @param LAVMappingID lavmapping id to be updated.
     * @param oldIRI actual iri.
     * @param newIRI new iri.
     */
    public void updateLavMappingSameAsFeature(String LAVMappingID, String oldIRI, String newIRI){

        LAVMappingR.update(LAVMappingMongo.FIELD_sameAsFeature.val(),oldIRI,LAVMappingMongo.FIELD_sameAsFeatureUpdate.val(), newIRI);

    }
}
