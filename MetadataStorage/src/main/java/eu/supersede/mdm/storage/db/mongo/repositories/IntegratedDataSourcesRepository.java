package eu.supersede.mdm.storage.db.mongo.repositories;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Updates;
import eu.supersede.mdm.storage.db.mongo.MongoConnection;
import eu.supersede.mdm.storage.db.mongo.models.IntegratedDataSourcesModel;
import eu.supersede.mdm.storage.db.mongo.models.fields.IntegratedDataSourceMongo;
import eu.supersede.mdm.storage.db.mongo.utils.UtilsMongo;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class IntegratedDataSourcesRepository {

    private MongoCollection<IntegratedDataSourcesModel> integratedDSCollection;


    @PostConstruct
    public void init() {
        integratedDSCollection = MongoConnection.getInstance().getDatabase().getCollection("integratedDataSources", IntegratedDataSourcesModel.class);
    }

    public void create(String strIDS){
        try {
            IntegratedDataSourcesModel integratedDS = UtilsMongo.mapper.readValue(strIDS, IntegratedDataSourcesModel.class);
            integratedDSCollection.insertOne(integratedDS);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (com.mongodb.MongoWriteException ex){
            //TODO: Handle error when not able to write in db and check other exception throw by insertOne
        }
    }

    public JSONObject create(JSONObject objBody){

        create(objBody.toJSONString());
        return objBody;
    }

    public List<IntegratedDataSourcesModel> findAll(){
        List<IntegratedDataSourcesModel> integratedDataSources = new ArrayList();
        MongoCursor cur = integratedDSCollection.find().iterator();
        while(cur.hasNext()) {
            integratedDataSources.add((IntegratedDataSourcesModel)cur.next());
        }
        return integratedDataSources;
    }

    public IntegratedDataSourcesModel findByField(String field, String value){
        return integratedDSCollection.find(eq(field,value)).first();
    }

    public IntegratedDataSourcesModel findByDataSourceID(String dataSourceID){
        return integratedDSCollection.find(eq(IntegratedDataSourceMongo.FIELD_dataSourceID.val(),dataSourceID)).first();
    }

    public void updateByDataSourceID(String dataSourceID, String field, String value){
        integratedDSCollection.updateOne(eq(IntegratedDataSourceMongo.FIELD_dataSourceID.val(),dataSourceID), Updates.set(field,value));
    }
    public void updateByDataSourceID(String dataSourceID, String field, JSONArray value){
        integratedDSCollection.updateOne(eq(IntegratedDataSourceMongo.FIELD_dataSourceID.val(),dataSourceID), Updates.set(field,value));
    }

    public void update(String queryField,String queryValue, String field, String value){
        integratedDSCollection.updateOne(eq(queryField,queryValue), Updates.set(field,value));
    }

    public void deleteByDataSourceID(String dataSourceID){
        integratedDSCollection.deleteOne(eq(IntegratedDataSourceMongo.FIELD_dataSourceID.val(), dataSourceID));
    }

}
