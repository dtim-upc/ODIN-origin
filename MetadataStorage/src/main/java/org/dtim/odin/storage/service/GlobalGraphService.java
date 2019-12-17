package org.dtim.odin.storage.service;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.mongo.models.GlobalGraphModel;
import org.dtim.odin.storage.db.mongo.models.LAVsameAs;
import org.dtim.odin.storage.db.mongo.models.fields.DataSourceMongo;
import org.dtim.odin.storage.db.mongo.models.fields.GlobalGraphMongo;
import org.dtim.odin.storage.db.mongo.models.fields.LAVMappingMongo;
import org.dtim.odin.storage.db.mongo.repositories.DataSourceRepository;
import org.dtim.odin.storage.db.mongo.repositories.GlobalGraphRepository;
import org.dtim.odin.storage.db.mongo.repositories.LAVMappingRepository;
import org.dtim.odin.storage.db.mongo.repositories.WrapperRepository;
import org.dtim.odin.storage.errorhandling.exception.DeleteNodeGlobalGException;
import org.dtim.odin.storage.service.model.LavObj;

import java.util.ArrayList;
import java.util.List;

public class GlobalGraphService {
    GlobalGraphRepository globalGraphR = new GlobalGraphRepository();

    LAVMappingRepository LAVMappingR = new LAVMappingRepository();

    WrapperRepository wrapperR = new WrapperRepository();

    DataSourceRepository dataSourceR = new DataSourceRepository();

    GraphOperations graphO = new GraphOperations();

    LAVMappingService delLAV = new LAVMappingService();

    public void deleteGlobalGraph(String globalGraphID){

        GlobalGraphModel globalGraphObj = globalGraphR.findByGlobalGraphID(globalGraphID);


        LAVMappingR.findAllByField(GlobalGraphMongo.FIELD_GlobalGraphID.val(),globalGraphID).forEach(lavMappingObj->{
            delLAV.delete(lavMappingObj.getLAVMappingID());
        });
        // Remove its named graph
        graphO.removeGraph(globalGraphObj.getNamedGraph());

        // Remove its metadata from MongoDB
        globalGraphR.deleteByGlobalGraphID(globalGraphID);
    }

    /*
     * Deletes a node.
     * First check if node is contained in the mappings, wrapper, datasource. If yes, we cannot delete it and  throws DeleteNodeGlobalGException.
     * We delete it from globalgraph. Need to update graphical graph.
     * Returns true if node is deleted.
     */
    public boolean deleteNode(String namedGraph, String nodeIRI) throws DeleteNodeGlobalGException {

        List<String> ls = new ArrayList<>();
        ls.add(nodeIRI);
        List<LavObj> lsLavs = getLavMappingsRelated(namedGraph,ls,true);

        if (!lsLavs.isEmpty()) {
            //check wrapper and datasource
            lsLavs.forEach( obj -> {
                if (GraphContains(obj.getWrapperIRI(), nodeIRI) || GraphContains(obj.getDataSourceIRI(), nodeIRI)){
                    throw new DeleteNodeGlobalGException("Cannot be deleted",this.getClass().getName(),"The element is contained in mappings");
                }
            });
        }

        graphO.deleteTriplesWithSubject(namedGraph,nodeIRI);
        graphO.deleteTriplesWithObject(namedGraph, nodeIRI);

        return true;
    }


    public boolean deleteProperty(String namedGraph, String sIRI, String pIRI, String oIRI){

        List<String> ls = new ArrayList<>();
        ls.add(sIRI);
        ls.add(oIRI);
        List<LavObj> lsLavs = getLavMappingsRelated(namedGraph,ls,false);

        if (!lsLavs.isEmpty()) {
            lsLavs.forEach( obj -> {
                if (GraphContainsTriples(obj.getWrapperIRI(),sIRI,pIRI,oIRI) || GraphContainsTriples(obj.getDataSourceIRI(),sIRI,pIRI,oIRI)){
                    throw new DeleteNodeGlobalGException("Cannot be deleted",this.getClass().getName(),"The element is contained in mappings");
                }
            });
        }

        graphO.deleteTriples(namedGraph,sIRI,pIRI,oIRI);
        return true;
    }


    /**
     * Check if a graph contains a given iri.
     * @param graphIRI graph iri
     * @param nodeIRI iri node to look in the graph
     * @return true if the graph contains the iri. Otherwise false.
     */
    public Boolean GraphContains(String graphIRI, String nodeIRI){
        if (graphO.containsIRIAsSubject(graphIRI,nodeIRI) && graphO.containsIRIAsObject(graphIRI,nodeIRI))
            return true;
        return false;
    }

    public Boolean GraphContainsTriples(String graphIRI, String sIRI, String pIRI, String oIRI){
        return graphO.containsTriples(graphIRI,sIRI,pIRI,oIRI);
    }

    /**
     * Gets the LavMappings IRI, wrapperIri and datasourceIRI related to a globalgraph
     *
     * @param namedGraph is the named Graph iri
     * @return a list for every lavmapping associated with the given globalgraph and need to be updated.
     */
    public List<LavObj> getLavMappingsRelated(String namedGraph, List<String> IRIs, Boolean verifyIRIs){

        // Array which contains the wrapper iri for the lavmapping.
        List<LavObj> listLWD = new ArrayList<>();

        String globalGraphID = globalGraphR.findByNamedGraph(namedGraph).getGlobalGraphID();

        LAVMappingR.findAllByField(LAVMappingMongo.FIELD_globalGraphID.val(),globalGraphID).forEach(lavMappingObj->{
            String lavMappingID = lavMappingObj.getLAVMappingID();
            String wrapperIri = wrapperR.findByWrapperID(lavMappingObj.getWrapperID()).getIri();
            String datasourceIri =dataSourceR.findByField(
                    DataSourceMongo.FIELD_Wrappers.val(),lavMappingObj.getWrapperID()).getIri();

            listLWD.add(new LavObj(lavMappingID,wrapperIri,datasourceIri));

            if(verifyIRIs){
                //Gets related datasource, wrapper and mapping that also need to be updated
                lavMappingObj.getSameAs().forEach(el->{
                    String feature = el.getFeature();

                    if(IRIs.contains(feature)){
                        throw new DeleteNodeGlobalGException("Cannot be deleted",this.getClass().getName(),"The element is contained in mappings");
                    }
                });

            }
        });
        return listLWD;
    }

    //
    //  Update operations
    //

    public Boolean updateTriples(JSONObject changes, String namedGraph) {

        if (changes.containsKey("nodes")) {

            List<String> currentIris = new ArrayList<>();
            ((JSONArray)changes.get("nodes")).forEach(el -> currentIris.add(((JSONObject)el).getAsString("old")));

            // Check which mappings contains the modified iri.
            List<LavObj> LavM = getLavMappingsAffected(getGlobalGraphId(namedGraph),currentIris);

            for (Object selectedElement : ((JSONArray) changes.get("nodes"))) {
                JSONObject objSelectedElement = (JSONObject) selectedElement;
                String oldIRI = objSelectedElement.getAsString("old");
                String newIRI = objSelectedElement.getAsString("new");

                updateNodeIri(namedGraph, oldIRI, newIRI);

                if (!LavM.isEmpty()) {
                    LavM.forEach( obj -> {
                        updateLavMapping(obj.getLAVMappingID(),oldIRI,newIRI);
                        updateNodeIri(obj.getWrapperIRI(), oldIRI, newIRI);
                        updateNodeIri(obj.getDataSourceIRI(),oldIRI,newIRI);
                    });
                }
            }
        }

        if (changes.containsKey("properties")) {
            ((JSONArray)changes.get("properties")).forEach(selectedElement -> {
                JSONObject objSelectedElement = (JSONObject)selectedElement;
                String pOldIRI = objSelectedElement.getAsString("pOld");
                String pNewIRI = objSelectedElement.getAsString("pNew");

                updatePropertyIri(namedGraph,pOldIRI,pNewIRI);

            });
        }

        if(changes.containsKey("new")){
            ((JSONArray)changes.get("new")).forEach(selectedElement -> {
                JSONObject objSelectedElement = (JSONObject)selectedElement;
                graphO.addTriple(namedGraph,objSelectedElement);
            });
        }

        if(changes.containsKey("changeNodeType")){

            List<String> currentIris = new ArrayList<>();
            ((JSONArray)changes.get("changeNodeType")).forEach(el -> currentIris.add(((JSONObject)el).getAsString("s")));
            // Check which mappings contains the change node.
            List<LavObj> LavM = getLavMappingsAffected(getGlobalGraphId(namedGraph),currentIris);

            ((JSONArray)changes.get("changeNodeType")).forEach(selectedElement -> {
                JSONObject objSelectedElement = (JSONObject)selectedElement;
                String operation = objSelectedElement.getAsString("operation");

                if(operation.equals("add")){
                    graphO.addTriple(namedGraph,objSelectedElement);
                }else{
                    graphO.deleteTriples(namedGraph,objSelectedElement);
                }


                if (!LavM.isEmpty()) {
                    LavM.forEach( obj -> {
                        if(operation.equals("add")){
                            graphO.addTriple(obj.getWrapperIRI(),objSelectedElement);
                        }else{
                            graphO.addTriple(obj.getWrapperIRI(),objSelectedElement);
                        }
                    });
                }

            });
        }

        return null;
    }

    /**
     * Gets the LavMappings IRI, wrapperIri and datasourceIRI which contains the features IRIs to be updated.
     *
     * @param globalGraphId
     * @return a list for every lavmapping associated with the given globalgraph and need to be updated.
     */
    public List<LavObj> getLavMappingsAffected(String globalGraphId, List<String> IRIs){

        // Array which contains the wrapper iri for the lavmapping.
        List<LavObj> listLWD = new ArrayList<>();

        LAVMappingR.findAllByField(LAVMappingMongo.FIELD_globalGraphID.val(),globalGraphId).forEach(lavMappingObj->{
            //Gets related datasource, wrapper and mapping that also need to be updated
            List<LAVsameAs> list = lavMappingObj.getSameAs();

            //Gets related datasource, wrapper and mapping that also need to be updated
            for (LAVsameAs el : list) {

                String feature = el.getFeature();

                if(IRIs.contains(feature)){
                    String lavMappingID = lavMappingObj.getLAVMappingID();
                    String wrapperIri = wrapperR.findByWrapperID(lavMappingObj.getWrapperID()).getIri();
                    String datasourceIri = dataSourceR.findByField(DataSourceMongo.FIELD_Wrappers.val(),
                            lavMappingObj.getWrapperID()).getIri();


                    listLWD.add(new LavObj(lavMappingID,wrapperIri,datasourceIri));
                    //we stop for each loop since we already identify we need to update these objects.
                    break;
                }
            };


        });

        return listLWD;
    }


    /**
     * Updates the feature IRI from sameAs key in LavMappings
     * @param LAVMappingID
     * @param oldIRI actual iri.
     * @param newIRI new iri.
     */
    public void updateLavMapping(String LAVMappingID, String oldIRI, String newIRI){

        LAVMappingR.update(LAVMappingMongo.FIELD_sameAsFeature.val(),oldIRI,
                LAVMappingMongo.FIELD_sameAsFeatureUpdate.val(),newIRI);

    }

    /**
     * Delete triple with oldIri and insert new triple with newIri in jena graph
     * @param graphIRI iri of the graph that needs to be updated.
     * @param oldIRI actual iri that appears in the triples.
     * @param newIRI new iri that is going to replace the actual iri.
     */
    public void updatePropertyIri(String graphIRI, String oldIRI, String newIRI){
        graphO.runAnUpdateQuery("WITH <"+graphIRI+"> DELETE {?s <"+oldIRI+"> ?o} " +
                "INSERT {?s <"+newIRI+"> ?o } WHERE {  ?s <"+oldIRI+"> ?o }");
    }

    /**
     * Delete triple with oldIri and insert new triple with newIri in jena graph
     * @param graphIRI iri of the graph that needs to be updated.
     * @param oldIRI actual iri that appears in the triples.
     * @param newIRI new iri that is going to replace the actual iri.
     */
    public void updateNodeIri(String graphIRI, String oldIRI, String newIRI){
        // Look and update triples where oldIRI is object.
        graphO.runAnUpdateQuery("WITH <"+graphIRI+">  DELETE {?s ?p <"+oldIRI+">} " +
                "INSERT {?s ?p <"+newIRI+">} WHERE {  ?s ?p <"+oldIRI+"> }");
        // Look and update triples where oldIRI is subject.
        graphO.runAnUpdateQuery("WITH <"+graphIRI+">  DELETE {<"+oldIRI+"> ?p ?o} " +
                "INSERT {<"+newIRI+"> ?p ?o} WHERE {  <"+oldIRI+"> ?p ?o }");
    }

    /**
     * Gets the global graph id.
     * @param namedGraph is the graph iri
     * @return a string that represent the global graph id.
     */
    public String getGlobalGraphId(String namedGraph){

        String globalGraphId = globalGraphR.findByNamedGraph(namedGraph).getGlobalGraphID();

        return globalGraphId;
    }

}
