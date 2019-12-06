package eu.supersede.mdm.storage.service;

import eu.supersede.mdm.storage.db.jena.GraphOperations;
import eu.supersede.mdm.storage.db.mongo.models.DataSourceModel;
import eu.supersede.mdm.storage.db.mongo.models.LAVMappingModel;
import eu.supersede.mdm.storage.db.mongo.models.fields.LAVMappingMongo;
import eu.supersede.mdm.storage.db.mongo.repositories.DataSourceRepository;
import eu.supersede.mdm.storage.db.mongo.repositories.LAVMappingRepository;
import eu.supersede.mdm.storage.db.mongo.repositories.WrapperRepository;

import javax.inject.Inject;
import java.util.List;

public class DataSourceService {
    @Inject
    LAVMappingRepository LAVMappingR;

    @Inject
    DataSourceRepository dataSourceR;

    @Inject
    WrapperRepository wrapperR;

    @Inject
    GraphOperations graphO;

    public void delete(String dataSourceID){

        DataSourceModel dataSourceObject = dataSourceR.findByDataSourceID(dataSourceID);

        WrapperService delW = new WrapperService();
        LAVMappingService delLAV = new LAVMappingService();

        List<String> wrappers =  dataSourceObject.getWrappers();

        // For all involved wrappers, apply the same logic as the wrapper removal.
        for (int i = 0; i < wrappers.size(); i++) {
            String wrapperID = (String) wrappers.get(i);

            LAVMappingModel LAVMappingObj = LAVMappingR.findByField(LAVMappingMongo.FIELD_wrapperID.val(),wrapperID);
            String wrapperIRI = wrapperR.findByWrapperID(wrapperID).getIri();
            if(wrapperIRI != null)
                graphO.removeGraph(wrapperIRI);
            if(LAVMappingObj != null){

                delLAV.removeLAVMappingFromMongo(LAVMappingObj.getLAVMappingID());
            }
            delW.removeWrapperMongo(wrapperID);
        }
        //Remove its named graph
        graphO.removeGraph(dataSourceObject.getIri());

        // Remove its metadata from MongoDB
        dataSourceR.deleteByDataSourceID(dataSourceID);
    }
}
