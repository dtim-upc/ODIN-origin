package eu.supersede.mdm.storage.db.mongo.repositories;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Updates;
import eu.supersede.mdm.storage.db.mongo.MongoConnection;
import eu.supersede.mdm.storage.db.mongo.models.DataSourceModel;
import eu.supersede.mdm.storage.db.mongo.models.fields.DataSourceMongo;
import eu.supersede.mdm.storage.db.mongo.utils.UtilsMongo;
import eu.supersede.mdm.storage.model.metamodel.SourceGraph;
import net.minidev.json.JSONObject;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;

public class DataSourceRepository {


    private MongoCollection<DataSourceModel> dataSourceCollection;

    @PostConstruct
    public void init() {
        dataSourceCollection = MongoConnection.getInstance().getDatabase().getCollection("dataSources", DataSourceModel.class);
    }

    public List<DataSourceModel> findAll(){
        List<DataSourceModel> dataS = new ArrayList();
        MongoCursor cur = dataSourceCollection.find().iterator();
        while(cur.hasNext()) {
            dataS.add((DataSourceModel)cur.next());
        }
        return dataS;
    }

    public DataSourceModel findByField(String field, String value){
        return dataSourceCollection.find(eq(field,value)).first();
    }

    public DataSourceModel findByDataSourceID(String dataSourceID){
        return dataSourceCollection.find(eq(DataSourceMongo.FIELD_DataSourceID.val(),dataSourceID)).first();
    }

    public void create(String dataSourceStr){
        try {
            DataSourceModel dataSource = UtilsMongo.mapper.readValue(dataSourceStr, DataSourceModel.class);
            dataSourceCollection.insertOne(dataSource);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (com.mongodb.MongoWriteException ex){
            //TODO:  Handle error when not able to write in db and check other exception throw by insertOne
            //if not possible to create should throw error and not write in jena
        }
    }

    public JSONObject create(JSONObject objBody){

        String dsName = objBody.getAsString("name").trim().replace(" ","");
        String iri = SourceGraph.DATA_SOURCE.val()+"/"+dsName;
        //Save metadata
        objBody.put("dataSourceID", UUID.randomUUID().toString().replace("-",""));
        objBody.put("iri", iri);
        objBody.put("bootstrappingType", "manual");

        create(objBody.toJSONString());

        return objBody;
    }

    public void addWrapper(String dataSourceID,String wrapperID ){
        dataSourceCollection.updateOne(eq(DataSourceMongo.FIELD_DataSourceID.val(),dataSourceID),
                Updates.addToSet(DataSourceMongo.FIELD_Wrappers.val() ,wrapperID) );
    }

    public void deleteByDataSourceID(String dataSourceID){
        dataSourceCollection.deleteOne(eq(DataSourceMongo.FIELD_DataSourceID.val(), dataSourceID));
    }

    /**
     * removes the element in the wrappers array attribute.
     */
    public void deleteOneWrapperFromDataSource(String dataSourceID,String wrapperID){
        dataSourceCollection.updateOne(eq(DataSourceMongo.FIELD_DataSourceID.val(),dataSourceID),
                Updates.pull(DataSourceMongo.FIELD_Wrappers.val(),wrapperID));
    }


}
