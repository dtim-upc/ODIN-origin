package org.dtim.odin.storage.resources;

import com.mongodb.MongoClient;
import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.dtim.odin.storage.db.jena.JenaConnection;
import org.dtim.odin.storage.db.mongo.utils.UtilsMongo;
import org.dtim.odin.storage.util.ConfigManager;
import org.dtim.odin.storage.util.Utils;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;

/**
 * Created by snadal on 17/05/16.
 */
@Path("metadataStorage")
public class AdminResource {



    /** System Metadata **/
    @GET @Path("admin/deleteAll")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_admin_delete_all() {
        System.out.println("[GET /admin/deleteAll/");
//        MongoClient client = Utils.getMongoDBClient();
//        client.getDatabase(ConfigManager.getProperty("system_metadata_db_name")).drop();

        UtilsMongo.dropMongoDB();
        try {
            JenaConnection.getInstance().close();
            FileUtils.deleteDirectory(new File(ConfigManager.getProperty("metadata_db_file")));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Response.ok("OK").build();
    }

    @GET @Path("admin/demoPrepare")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_admin_demoprepare() {
        System.out.println("[GET /admin/demoPrepare/");
        MongoClient client = Utils.getMongoDBClient();
        Bson command = new Document("eval","db.copyDatabase(\"MDM_MetadataStorage_WISCENTD\",\"MDM_MetadataStorage\",\"localhost\")");
        client.getDatabase("MDM_MetadataStorage_WISCENTD").runCommand(command);

        try {
            FileUtils.copyDirectory(new File("/home/snadal/UPC/Projects/MDM/WISCENTD_bkp/MDM_TDB"),
                    new File("/home/snadal/UPC/Projects/MDM/MetadataStorage/MDM_TDB"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Response.ok("OK").build();
    }

}