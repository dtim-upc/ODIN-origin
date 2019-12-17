package org.dtim.odin.storage.service;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.mongo.models.DataSourceModel;
import org.dtim.odin.storage.db.mongo.models.LAVMappingModel;
import org.dtim.odin.storage.db.mongo.models.WrapperModel;
import org.dtim.odin.storage.db.mongo.models.fields.LAVMappingMongo;
import org.dtim.odin.storage.db.mongo.models.fields.WrapperMongo;
import org.dtim.odin.storage.db.mongo.repositories.DataSourceRepository;
import org.dtim.odin.storage.db.mongo.repositories.LAVMappingRepository;
import org.dtim.odin.storage.db.mongo.repositories.WrapperRepository;
import org.dtim.odin.storage.errorhandling.exception.AttributesExistWrapperException;
import org.dtim.odin.storage.model.Namespaces;
import org.dtim.odin.storage.model.metamodel.SourceGraph;
import org.dtim.odin.storage.resources.WrapperResource;

public class WrapperService {

    LAVMappingRepository LAVMappingR = new LAVMappingRepository();

    WrapperRepository wrapperR = new WrapperRepository();

    DataSourceRepository dataSourceR = new DataSourceRepository();

    GraphOperations graphO = new GraphOperations();

    LAVMappingService delLAV = new LAVMappingService();

    public void verifyAttributesCreation(JSONArray attributes){

        attributes.forEach(attribute -> {

            WrapperModel result = wrapperR.findByField(WrapperMongo.FIELD_attributesName.val(),((JSONObject) attribute).getAsString("name"));

            if(result != null)//there is another wrapper with the same attribute
                    throw new AttributesExistWrapperException("Wrapper can not be created", WrapperResource.class.getName(),"The attribute "+(((JSONObject) attribute).getAsString("name"))+" already exist in wrapper: "+result.getName());
        });
    }


    public JSONObject createWrapper(String body) {
        JSONObject objBody = (JSONObject) JSONValue.parse(body);

        verifyAttributesCreation((JSONArray) objBody.get("attributes"));

        objBody = wrapperR.create(objBody);

        //Update the data source with the new wrapper
        dataSourceR.addWrapper(objBody.getAsString("dataSourceID"),objBody.getAsString("wrapperID"));

        //RDF - we use as named graph THE SAME as the data source
        String dsIRI =  dataSourceR.findByDataSourceID(objBody.getAsString("dataSourceID")).getIri();
        String wIRI = objBody.getAsString("iri");

        graphO.addTriple(dsIRI, wIRI, Namespaces.rdf.val() + "type", SourceGraph.WRAPPER.val());
        graphO.addTriple(dsIRI, dsIRI, SourceGraph.HAS_WRAPPER.val(), wIRI);
        ((JSONArray) objBody.get("attributes")).forEach(attribute -> {
            String attName = ((JSONObject) attribute).getAsString("name");
            String attIRI = dsIRI + "/" + attName/*.trim().replace(" ", "")*/;
            graphO.addTriple(dsIRI, attIRI, Namespaces.rdf.val() + "type", SourceGraph.ATTRIBUTE.val());
            graphO.addTriple(dsIRI, wIRI, SourceGraph.HAS_ATTRIBUTE.val(), attIRI);

        });

        return objBody;
    }

//    public JSONObject createWrapperBDI(String body) {
//        JSONObject objBody = (JSONObject) JSONValue.parse(body);
//
//        objBody = wrapperR.create(objBody);
//
//        //Update the data source with the new wrapper
//        dataSourceR.addWrapper(objBody.getAsString("dataSourceID"),objBody.getAsString("wrapperID"));
//
//        String dsIRI =  dataSourceR.findByDataSourceID(objBody.getAsString("dataSourceID")).getIri();
//        String wIRI = objBody.getAsString("iri");
//
//        graphO.addTriple(dsIRI, wIRI, Namespaces.rdf.val() + "type", SourceGraph.WRAPPER.val());
//        graphO.addTriple(dsIRI, dsIRI, SourceGraph.HAS_WRAPPER.val(), wIRI);
//        ((JSONArray) objBody.get("attributes")).forEach(attribute -> {
//            String attName = ((JSONObject) attribute).getAsString("name");
//            String attIRI = dsIRI + "/" + attName/*.trim().replace(" ", "")*/;
//            graphO.addTriple(dsIRI, attIRI, Namespaces.rdf.val() + "type", SourceGraph.ATTRIBUTE.val());
//            graphO.addTriple(dsIRI, wIRI, SourceGraph.HAS_ATTRIBUTE.val(), attIRI);
//
//        });
//
//        return objBody;
//    }


    // delete

    public void delete(String wrapperID){

        WrapperModel wrapperObject = wrapperR.findByWrapperID(wrapperID);
        DataSourceModel dataSourceObject = dataSourceR.findByDataSourceID(wrapperObject.getDataSourceID());
         delete(wrapperObject,dataSourceObject);
    }

    public void delete(String wrapperID, DataSourceModel dataSourceObject){
//        Document wrapperObject = ServiceUtils.getWrapper(new Document("wrapperID",wrapperID));
        WrapperModel wrapperObject= wrapperR.findByWrapperID(wrapperID);
        delete(wrapperObject,dataSourceObject);
    }

    public void delete(WrapperModel wrapperObject,DataSourceModel dataSourceObject){
        // Remove the triples from the source graph
        graphO.deleteTriplesWithSubject(dataSourceObject.getIri(),wrapperObject.getIri());
        graphO.deleteTriplesWithObject(dataSourceObject.getIri(),wrapperObject.getIri());

        //Remove its LAV mapping if exists & Update the metadata for the affected data source in MongoDB
        LAVMappingModel LAVMappingObj = LAVMappingR.findByField(LAVMappingMongo.FIELD_wrapperID.val(),wrapperObject.getWrapperID());
        if(LAVMappingObj != null){
            delLAV.delete(LAVMappingObj,wrapperObject,dataSourceObject);
        }
        //Remove its metadata from MongoDB

        dataSourceR.deleteOneWrapperFromDataSource( dataSourceObject.getDataSourceID(),wrapperObject.getWrapperID());

        removeWrapperMongo(wrapperObject.getWrapperID());
    }

    public void removeWrapperMongo(String id){
        wrapperR.deleteByWrapperID(id);
    }
}
