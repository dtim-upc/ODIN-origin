package org.dtim.odin.storage.service;

import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.mongo.models.DataSourceModel;
import org.dtim.odin.storage.db.mongo.models.LAVMappingModel;
import org.dtim.odin.storage.db.mongo.models.fields.LAVMappingMongo;
import org.dtim.odin.storage.db.mongo.repositories.DataSourceRepository;
import org.dtim.odin.storage.db.mongo.repositories.LAVMappingRepository;
import org.dtim.odin.storage.db.mongo.repositories.WrapperRepository;

import java.util.List;

public class DataSourceService {
    LAVMappingRepository LAVMappingR = new LAVMappingRepository();

    DataSourceRepository dataSourceR = new DataSourceRepository();

    WrapperRepository wrapperR = new WrapperRepository();

    GraphOperations graphO = GraphOperations.getInstance();

    LAVMappingService LAVMappingService = new LAVMappingService();

    WrapperService wrapperService =new WrapperService();

    public void delete(String dataSourceID){

        DataSourceModel dataSourceObject = dataSourceR.findByDataSourceID(dataSourceID);
        List<String> wrappers =  dataSourceObject.getWrappers();

        // For all involved wrappers, apply the same logic as the wrapper removal.
        for (int i = 0; i < wrappers.size(); i++) {
            String wrapperID = (String) wrappers.get(i);

            LAVMappingModel LAVMappingObj = LAVMappingR.findByField(LAVMappingMongo.FIELD_wrapperID.val(),wrapperID);
            String wrapperIRI = wrapperR.findByWrapperID(wrapperID).getIri();
            if(wrapperIRI != null)
                graphO.removeGraph(wrapperIRI);
            if(LAVMappingObj != null){

                LAVMappingService.removeLAVMappingFromMongo(LAVMappingObj.getLAVMappingID());
            }
            wrapperService.removeWrapperMongo(wrapperID);
        }
        //Remove its named graph
        graphO.removeGraph(dataSourceObject.getIri());

        // Remove its metadata from MongoDB
        dataSourceR.deleteByDataSourceID(dataSourceID);
    }
}
