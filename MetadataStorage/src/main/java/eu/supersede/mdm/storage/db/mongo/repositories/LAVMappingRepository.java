package eu.supersede.mdm.storage.db.mongo.repositories;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import eu.supersede.mdm.storage.db.mongo.MongoConnection;
import eu.supersede.mdm.storage.db.mongo.models.LAVMappingModel;
import eu.supersede.mdm.storage.db.mongo.models.fields.LAVMappingMongo;
import eu.supersede.mdm.storage.db.mongo.utils.UtilsMongo;
import net.minidev.json.JSONObject;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;

public class LAVMappingRepository {

    private MongoCollection<LAVMappingModel> LAVCollection;

    public LAVMappingRepository() {
        LAVCollection = MongoConnection.getInstance().getDatabase().getCollection("LAVMappings", LAVMappingModel.class);
    }

    public List<LAVMappingModel> findAll(){
        List<LAVMappingModel> LAVMappings = new ArrayList();
        MongoCursor cur = LAVCollection.find().iterator();
        while(cur.hasNext()) {
            LAVMappings.add((LAVMappingModel)cur.next());
        }
        return LAVMappings;
    }

    public LAVMappingModel findByField(String field, String value){
        return LAVCollection.find(eq(field,value)).first();
    }

    public List<LAVMappingModel> findAllByField(String field, String value){
        List<LAVMappingModel> LAVMappings = new ArrayList();
        MongoCursor cur = LAVCollection.find(eq(field,value)).iterator();
        while(cur.hasNext()) {
            LAVMappings.add((LAVMappingModel)cur.next());
        }
        return LAVMappings;
    }

    public LAVMappingModel findByLAVMappingID(String LAVMappingIDStr){
        return LAVCollection.find(eq(LAVMappingMongo.FIELD_LAVMappingID.val(),LAVMappingIDStr)).first();
    }


    public void create(String strLAVmapping){
        try {
            LAVMappingModel mapping = UtilsMongo.mapper.readValue(strLAVmapping, LAVMappingModel.class);
            LAVCollection.insertOne(mapping);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (com.mongodb.MongoWriteException ex){
            //TODO:  Handle error when not able to write in db and check other exception throw by insertOne
        }
    }

    public JSONObject create(JSONObject objBody){

        objBody.put("LAVMappingID", UUID.randomUUID().toString().replace("-", ""));
        create(objBody.toJSONString());

        return objBody;
    }

    /**
     * Update the value of the field provided in LAVMapping collection using the id provided
     * @param LAVmappingID
     * @param field
     * @param value
     */
    public void updateByLAVMappingID(String LAVmappingID, String field, String value){
        LAVCollection.updateOne(eq(LAVMappingMongo.FIELD_LAVMappingID.val(),LAVmappingID), Updates.set(field,value));
    }

    public void update(String queryField,String queryValue, String field, String value){
        LAVCollection.updateOne(eq(queryField,queryValue), Updates.set(field,value));
    }

    public DeleteResult deleteByLAVMappingID(String LAVmappingID){
        return LAVCollection.deleteOne(eq(LAVMappingMongo.FIELD_LAVMappingID.val(),LAVmappingID));
    }

    public void deleteField(String LAVMappingID,String field){
        LAVCollection.updateOne(eq(LAVMappingMongo.FIELD_LAVMappingID.val(),LAVMappingID),Updates.unset(field));
    }




}
