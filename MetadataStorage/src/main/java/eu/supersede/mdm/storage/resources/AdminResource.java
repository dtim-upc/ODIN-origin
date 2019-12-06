package eu.supersede.mdm.storage.resources;

import eu.supersede.mdm.storage.db.mongo.MongoConnection;
import eu.supersede.mdm.storage.util.ConfigManager;
import org.apache.commons.io.FileUtils;

import javax.inject.Inject;
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

    @Inject
    MongoConnection mongoConnection;

    /** System Metadata **/
    @GET @Path("admin/deleteAll")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_admin_delete_all() {
        System.out.println("[GET /admin/deleteAll/");
//        MongoClient client = Utils.getMongoDBClient();
//        client.getDatabase(ConfigManager.getProperty("system_metadata_db_name")).drop();

        mongoConnection.drop();
        try {
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
        /*System.out.println("[GET /admin/demoPrepare/");
        MongoClient client = Utils.getMongoDBClient();
        Bson command = new Document("eval","db.copyDatabase(\"edbt_copy\",\"MDM_MetadataStorage\",\"127.0.0.1\")");
        client.getDatabase("edbt_copy").runCommand(command);

        try {
            FileUtils.copyDirectory(new File("/home/snadal/UPC/Projects/MDM/MetadataStorage/MDM_TDBMDM_TDB_edbt"),
                    new File("/home/snadal/UPC/Projects/MDM/MetadataStorage/MDM_TDBMDM_TDB"));
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        return Response.ok("OK").build();
    }

}