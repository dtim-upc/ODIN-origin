package eu.supersede.mdm.storage.db.mongo.repositories;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Updates;
import eu.supersede.mdm.storage.db.mongo.MongoConnection;
import eu.supersede.mdm.storage.db.mongo.models.GlobalGraphModel;
import eu.supersede.mdm.storage.db.mongo.models.fields.GlobalGraphMongo;
import eu.supersede.mdm.storage.db.mongo.utils.UtilsMongo;
import net.minidev.json.JSONObject;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;

public class GlobalGraphRepository {

    private MongoCollection<GlobalGraphModel> globalGraphCollection;


    @PostConstruct
    public void init() {
        globalGraphCollection = MongoConnection.getInstance().getDatabase().getCollection("globalGraphs", GlobalGraphModel.class);
    }

    public void create(String globalGraph){
        try {
            GlobalGraphModel globalG = UtilsMongo.mapper.readValue(globalGraph, GlobalGraphModel.class);
            globalGraphCollection.insertOne(globalG);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (com.mongodb.MongoWriteException ex){
            //TODO: Handle error when not able to write in db and check other exception throw by insertOne
        }
    }

    public void create(GlobalGraphModel globalGraph){
        try {
            globalGraphCollection.insertOne(globalGraph);
        } catch (com.mongodb.MongoWriteException ex){
            //TODO:Handle error when not able to write in db and check other exception throw by insertOne
        }
    }


    public JSONObject create(JSONObject globalGraphJsonObj){
        globalGraphJsonObj.put("globalGraphID", UUID.randomUUID().toString().replace("-",""));

        String namedGraph =
                globalGraphJsonObj.getAsString("defaultNamespace").charAt(globalGraphJsonObj.getAsString("defaultNamespace").length()-1) == '/' ?
                        globalGraphJsonObj.getAsString("defaultNamespace") : globalGraphJsonObj.getAsString("defaultNamespace") + "/";

        globalGraphJsonObj.put("namedGraph", namedGraph+UUID.randomUUID().toString().replace("-",""));

        create(globalGraphJsonObj.toJSONString());

        return globalGraphJsonObj;
    }

    public GlobalGraphModel findByGlobalGraphID(String globalGraphID){
        return globalGraphCollection.find(eq(GlobalGraphMongo.FIELD_GlobalGraphID.val(),globalGraphID)).first();
    }

    public GlobalGraphModel findByNamedGraph(String namedGraph){
        return globalGraphCollection.find(eq(GlobalGraphMongo.FIELD_NamedGraph.val(),namedGraph)).first();
    }

    public List<GlobalGraphModel> findAll(){
        List<GlobalGraphModel> globalGraphs = new ArrayList();
        MongoCursor cur = globalGraphCollection.find().iterator();
        while(cur.hasNext()) {
            globalGraphs.add((GlobalGraphModel)cur.next());
        }
        return globalGraphs;
    }

    public void updateByGlobalGraphID(String globalGraphID, String field, String value){
        globalGraphCollection.updateOne(eq(GlobalGraphMongo.FIELD_GlobalGraphID.val(),globalGraphID), Updates.set(field,value));
    }

    public void updateByGlobalGraphID(String globalGraphID, String field, Object value){
        globalGraphCollection.updateOne(eq(GlobalGraphMongo.FIELD_GlobalGraphID.val(),globalGraphID), Updates.set(field,value));
    }

    public void deleteByGlobalGraphID(String globalGraphID){
        globalGraphCollection.deleteOne(eq(GlobalGraphMongo.FIELD_GlobalGraphID.val(), globalGraphID));
    }

}
