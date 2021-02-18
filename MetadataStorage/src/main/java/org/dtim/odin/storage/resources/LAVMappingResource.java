package org.dtim.odin.storage.resources;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.dtim.odin.storage.db.mongo.models.LAVMappingModel;
import org.dtim.odin.storage.db.mongo.repositories.LAVMappingRepository;
import org.dtim.odin.storage.db.mongo.utils.UtilsMongo;
import org.dtim.odin.storage.service.LAVMappingService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.logging.Logger;

/**
 * Created by snadal on 22/11/16.
 */
@Path("metadataStorage")
public class LAVMappingResource {

    private static final Logger LOGGER = Logger.getLogger(LAVMappingResource.class.getName());

    LAVMappingRepository LAVMappingR = new LAVMappingRepository();

    LAVMappingService LAVMappingS = new LAVMappingService();

    @GET
    @Path("LAVMapping/")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_LAVMapping() {
        LOGGER.info("[GET /LAVMapping/]");

        String json = UtilsMongo.serializeListJsonAsString(LAVMappingR.findAll());
        return Response.ok(json).build();
    }

    @GET
    @Path("LAVMapping/{LAVMappingID}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_LAVMappingByID(@PathParam("LAVMappingID") String LAVMappingID) {
        LOGGER.info("[GET /LAVMapping/] LAVMappingID = " + LAVMappingID);

        LAVMappingModel mappping = LAVMappingR.findByLAVMappingID(LAVMappingID);
        if(mappping != null )
            return Response.ok(UtilsMongo.ToJsonString(mappping)).build();
        return Response.status(404).build();
    }

    @POST
    @Path("LAVMapping/sameAs")
    @Consumes("text/plain")
    public Response POST_LAVMappingMapsTo(String body) {
        LOGGER.info("[POST /LAVMapping/mapsTo/] body = " + body);
//        LAVMappingService LAVMapp = new LAVMappingService();
        JSONObject objBody = (JSONObject) JSONValue.parse(body);
        if (objBody.getAsString("isModified").equals("false")) {
            objBody = LAVMappingS.createLAVMappingMapsTo(body);
        } else {
            objBody = LAVMappingS.updateLAVMappingMapsTo(body);
        }
        return Response.ok(objBody.toJSONString()).build();
    }

    @POST
    @Path("LAVMapping/subgraph")
    @Consumes("text/plain")
    public Response POST_LAVMappingSubgraph(String body) {
        LOGGER.info("[POST /LAVMapping/subgraph/] body = " + body);
        JSONObject objBody = LAVMappingS.createLAVMappingSubgraph(body);
        return Response.ok(objBody.toJSONString()).build();
    }


    @ApiOperation(value = "Delete a LAVMapping",consumes = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK")})
    @DELETE @Path("LAVMapping/{LAVMappingID}")
    @Consumes("text/plain")
    public Response DELETE_LAVMappingByID(@PathParam("LAVMappingID") String LAVMappingID) {
        //TODO: if element LAVMappingID null throw error or logger
        LOGGER.info("[DELETE /LAVMapping/ "+LAVMappingID);
        LAVMappingS.delete(LAVMappingID);
        return Response.ok().build();
    }

}
