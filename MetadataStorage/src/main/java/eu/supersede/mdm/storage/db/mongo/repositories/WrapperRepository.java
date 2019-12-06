package eu.supersede.mdm.storage.db.mongo.repositories;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import eu.supersede.mdm.storage.db.mongo.MongoConnection;
import eu.supersede.mdm.storage.db.mongo.models.WrapperModel;
import eu.supersede.mdm.storage.db.mongo.models.fields.WrapperMongo;
import eu.supersede.mdm.storage.db.mongo.utils.UtilsMongo;
import eu.supersede.mdm.storage.model.metamodel.SourceGraph;
import net.minidev.json.JSONObject;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.mongodb.client.model.Filters.eq;

public class WrapperRepository {

    private MongoCollection<WrapperModel> wrapperCollection;


    @PostConstruct
    public void init() {
        wrapperCollection = MongoConnection.getInstance().getDatabase().getCollection("wrappers", WrapperModel.class);
    }

    public List<WrapperModel> findAll(){
        List<WrapperModel> wrappers = new ArrayList();
        MongoCursor cur = wrapperCollection.find().iterator();
        while(cur.hasNext()) {
            wrappers.add((WrapperModel)cur.next());
        }
        return wrappers;
    }

    public WrapperModel findByField(String field, String value){
        return wrapperCollection.find(eq(field,value)).first();
    }
    public WrapperModel findByWrapperID(String wrapperID){
        return wrapperCollection.find(eq(WrapperMongo.FIELD_wrapperID.val(),wrapperID)).first();
    }

    public void create(String wrapperStr){
        try {
            WrapperModel wrapper = UtilsMongo.mapper.readValue(wrapperStr, WrapperModel.class);
            wrapperCollection.insertOne(wrapper);
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (com.mongodb.MongoWriteException ex){
            //TODO:  Handle error when not able to write in db and check other exception throw by insertOne
        }
    }

    public JSONObject create(JSONObject objBody){

        //Metadata for the wrapper
        objBody.put("wrapperID", "w"+ UUID.randomUUID().toString().replace("-",""));
        String wrapperName = objBody.getAsString("name")/*.trim().replace(" ","")*/;
        String wIRI = SourceGraph.WRAPPER.val()+"/"+wrapperName;
        objBody.put("iri",wIRI);
        create(objBody.toJSONString());
        return objBody;
    }

    public void deleteByWrapperID(String wrapperID){
        wrapperCollection.deleteOne(eq(WrapperMongo.FIELD_wrapperID.val(), wrapperID));
    }


}
