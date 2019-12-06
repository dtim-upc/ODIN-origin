package eu.supersede.mdm.storage.db.mongo.repositories;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Updates;
import eu.supersede.mdm.storage.db.mongo.models.LAVMappingModel;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class repositoryTest {

    public static void main(String[] args) {
         MongoCollection<LAVMappingModel> LAVCollection;

        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoClientOptions options = MongoClientOptions.builder()
                .codecRegistry(pojoCodecRegistry)
                .writeConcern(WriteConcern.ACKNOWLEDGED) //check!! // To be able to wait for confirmation after writing on the DB
                .connectionsPerHost(4)
                .maxConnectionIdleTime((60 * 1_000))
                .maxConnectionLifeTime((120 * 1_000))
                .build();


        MongoClient mongo = new MongoClient("localhost",options);
        LAVCollection = mongo.getDatabase("MDM_MetadataStorage").getCollection("LAVMappings", LAVMappingModel.class);


        LAVCollection.updateOne(eq("sameAs.feature","http://www.BDIOntology.com/schema/WIDP-DrugDistribution/Period_year"), Updates.set("sameAs.$.feature","http://www.BDIOntology.com/schema/prueba"));


//
//        query.append("LAVMappingID","http://www.BDIOntology.com/schema/WIDP-DrugDistribution/Period_year")
//                .append("sameAs.feature", oldIRI);
//
//        Document setData = new Document();
//        setData.append("sameAs.$.feature", newIRI);
//
//        Document update = new Document();
//        update.append("$set", setData);
    }
}
