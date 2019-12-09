package eu.supersede.mdm.storage.resources;

import eu.supersede.mdm.storage.db.jena.GraphOperations;
import eu.supersede.mdm.storage.db.mongo.models.DataSourceModel;
import eu.supersede.mdm.storage.db.mongo.models.WrapperModel;
import eu.supersede.mdm.storage.db.mongo.repositories.DataSourceRepository;
import eu.supersede.mdm.storage.db.mongo.repositories.WrapperRepository;
import eu.supersede.mdm.storage.db.mongo.utils.UtilsMongo;
import eu.supersede.mdm.storage.model.metamodel.SourceGraph;
import eu.supersede.mdm.storage.model.omq.relational_operators.Wrapper;
import eu.supersede.mdm.storage.service.WrapperService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by snadal on 22/11/16.
 * Updated by Kashif Rabbani June 10 2019
 */
@Path("metadataStorage")
public class WrapperResource {

    private static final Logger LOGGER = Logger.getLogger(WrapperResource.class.getName());

    WrapperRepository wrapperR = new WrapperRepository();

    DataSourceRepository dataSourceR = new DataSourceRepository();

    WrapperService wrapperS = new WrapperService();

    GraphOperations graphO = new GraphOperations();

    @GET
    @Path("wrapper/")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_wrapper() {
        System.out.println("[GET /wrapper/]");

        String json = UtilsMongo.serializeListJsonAsString(wrapperR.findAll());
        return Response.ok(json).build();
    }

    @GET
    @Path("wrapper/{wrapperID}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_wrapperByID(@PathParam("wrapperID") String wrapperID) {
        System.out.println("[GET /wrapper/] wrapperID = " + wrapperID);

        WrapperModel wrapper = wrapperR.findByWrapperID(wrapperID);
        if(wrapper != null )
            return Response.ok(UtilsMongo.ToJsonString(wrapper)).build();
        return Response.status(404).build();
    }

    @POST
    @Path("wrapper/")
    @Consumes("text/plain")
    public Response POST_wrapper(String body) {
        System.out.println("[POST /wrapper/] body = " + body);
        JSONObject objBody = wrapperS.createWrapper(body);
        return Response.ok(objBody.toJSONString()).build();
    }

    @POST
    @Path("wrapper/inferSchema/")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response POST_inferSchema(String body) throws Exception {
        System.out.println("[POST /inferSchema/] body = " + body);
        JSONObject objBody = (JSONObject) JSONValue.parse(body);
        String query = objBody.getAsString("query");
        String dataSourceID = objBody.getAsString("dataSourceID");

        DataSourceModel ds = dataSourceR.findByDataSourceID(dataSourceID);

        Wrapper w = Wrapper.specializeWrapper(ds,query); //Body sent to get extra parameters
        return Response.ok((w.inferSchema())).build();
    }

    @POST
    @Path("wrapper/preview/")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response POST_preview(String body) throws Exception {
        System.out.println("[POST /preview/] body = " + body);
        JSONObject objBody = (JSONObject) JSONValue.parse(body);
        String dataSourceID = objBody.getAsString("dataSourceID");
        String query = objBody.getAsString("query");
        List<String> attributes = ((JSONArray)JSONValue.parse(objBody.getAsString("attributes")))
                .stream().map(a -> (String)a).collect(Collectors.toList());

        DataSourceModel ds = dataSourceR.findByDataSourceID(dataSourceID);

        Wrapper w = Wrapper.specializeWrapper(ds,query);

        return Response.ok((w.preview(attributes))).build();
    }

    @GET
    @Path("wrapper/{iri}/attributes")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_attributesForWrapper(@PathParam("iri") String iri) {
        System.out.println("[GET /wrapper/attributes/] iri = "+iri);
        JSONArray attributes = getWrapperAttributes(iri);
        return Response.ok(attributes.toJSONString()).build();
    }

    //TODO: (javier) pass method to service and delete method in MDMLAVMapping createLAVMapping function
    public JSONArray getWrapperAttributes(String iri) {
        JSONArray attributes = new JSONArray();
        String SPARQL = "SELECT ?a WHERE { GRAPH ?g { <"+iri+"> <"+ SourceGraph.HAS_ATTRIBUTE.val()+"> ?a } }";
        graphO.runAQuery(SPARQL).forEachRemaining(t -> {
            attributes.add(t.get("a").asNode().getURI());
        });
        return attributes;
    }
    @ApiOperation(value = "Delete a Wrapper and a LAVMapping if exist",consumes = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK")})
    @DELETE @Path("wrapper/{wrapperID}")
    @Consumes("text/plain")
    public Response DELETE_LAVMappingByID(@PathParam("wrapperID") String wrapperID) {
        LOGGER.info("[DELETE /wrapper/ "+wrapperID);
        WrapperService del =new WrapperService();
        del.delete(wrapperID);
        return Response.ok().build();
    }
}