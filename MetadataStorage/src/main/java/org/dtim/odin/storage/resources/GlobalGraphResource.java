package org.dtim.odin.storage.resources;

import io.swagger.annotations.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.mongo.models.GlobalGraphModel;
import org.dtim.odin.storage.db.mongo.models.fields.GlobalGraphMongo;
import org.dtim.odin.storage.db.mongo.repositories.GlobalGraphRepository;
import org.dtim.odin.storage.db.mongo.utils.UtilsMongo;
import org.dtim.odin.storage.parsers.ImportOWLtoGlobalGraph;
import org.dtim.odin.storage.service.GlobalGraphService;
import org.dtim.odin.storage.validator.GlobalGraphValidator;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.logging.Logger;

/**
 * Created by snadal on 22/11/16.
 */
@Api(value = "metadataStorage")
@Path("metadataStorage")
public class GlobalGraphResource {

    private static final Logger LOGGER = Logger.getLogger(GlobalGraphResource.class.getName());
    private static final String LOG_MSG =
            "{} request finished with inputs: {} and return value: {} in {}ms";
    GlobalGraphValidator validator = new GlobalGraphValidator();

//    @Inject
    GlobalGraphRepository globalGraphR = new GlobalGraphRepository();

//    @Inject
    GraphOperations graphO = new GraphOperations();

//    @Inject
    GlobalGraphService globalService = new GlobalGraphService();

    @ApiOperation(value = "Gets all global graphs registered",produces = MediaType.TEXT_PLAIN)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    @GET
    @Path("globalGraph/")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_globalGraph() {
        LOGGER.info("[GET /GET_globalGraph/]");
        String json = UtilsMongo.serializeListJsonAsString(globalGraphR.findAll());
        return Response.ok(json).build();
    }

    @ApiOperation(value = "Gets the information related for the given globalgraphid",produces = MediaType.TEXT_PLAIN)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK")})
    @GET
    @Path("globalGraph/{globalGraphID}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_globalGraphByID(
            @ApiParam(value = "global graph identifier", required = true)
            @PathParam("globalGraphID") String globalGraphID) {

        LOGGER.info("[GET /globalGraph/] globalGraphID = "+globalGraphID);

        GlobalGraphModel globalGraph = globalGraphR.findByGlobalGraphID(globalGraphID);
        if(globalGraph != null )
            return Response.ok(UtilsMongo.ToJsonString(globalGraph)).build();
        return Response.status(404).build();
    }

    @ApiOperation(value = "Gets the information related for the given namedGraph",produces = MediaType.TEXT_PLAIN)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK",examples = @Example(value = {@ExampleProperty(value = "{ \"namedGraph\" : \"http:/www.essi.upc.edu/us/SportsUML/d4c3dbea56d5493aad50788bd419552d\", \"defaultNamespace\" : \"http:/www.essi.upc.edu/us/SportsUML/\", \"name\" : \"SportsUML\", \"globalGraphID\" : \"60f4c99f29fd40d88c9842199b456e1a\", \"graphicalGraph\" : \"\" }\n")}))})
    @GET
    @Path("globalGraph/namedGraph/{namedGraph}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_globalGraphFromNamedGraph(
            @ApiParam(value = "global graph name", required = true)
            @PathParam("namedGraph") String namedGraph) {

        LOGGER.info("[GET /globalGraph/namedGraph/] namedGraph = "+namedGraph);

        GlobalGraphModel globalGraph = globalGraphR.findByNamedGraph(namedGraph);
        if(globalGraph != null )
            return Response.ok(UtilsMongo.ToJsonString(globalGraph)).build();
        return Response.status(404).build();
    }

    @ApiOperation(value = "Gets all features related for the given namedGraph",produces = MediaType.TEXT_PLAIN)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK",examples = @Example(value = {@ExampleProperty(value = "[\"http:/www.essi.upc.edu/us/SportsUML/feature1\",\"http:/www.essi.upc.edu/us/SportsUML/feature2\"]")}))})
    @GET
    @Path("globalGraph/{namedGraph}/features")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_featuresForGlobalGraph(
            @ApiParam(value = "global graph name", required = true)
            @PathParam("namedGraph") String namedGraph) {

        LOGGER.info("[GET /globalGraph/features/] namedGraph = "+namedGraph);
        JSONArray features = graphO.getFeaturesFromGraph(namedGraph);
        return Response.ok(features.toJSONString()).build();
    }


    @ApiOperation(value = "Gets all features with its concept related for the given namedGraph",produces = MediaType.TEXT_PLAIN)
    @ApiResponses(value = {@ApiResponse(code = 200, message = "OK",examples = @Example(value = {@ExampleProperty(value = "[\"http:/www.essi.upc.edu/us/SportsUML/feature1\",\"http:/www.essi.upc.edu/us/SportsUML/feature2\"]")}))})
    @GET
    @Path("globalGraph/{namedGraph}/featuresAndConcepts")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_FeaturesAndConceptsForGlobalGraph(
            @ApiParam(value = "global graph name", required = true)
            @PathParam("namedGraph") String namedGraph) {

        LOGGER.info("[GET /globalGraph/features/] namedGraph = "+namedGraph);
        JSONObject featureConcept = graphO.getFeaturesWithConceptFromGraph(namedGraph);
        return Response.ok(featureConcept.toJSONString()).build();
    }

    @ApiOperation(value = "Create a new global graph",produces = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK", examples = @Example(value = {@ExampleProperty(value = "{\"namedGraph\":\"http:\\/namespace\\/example\\/8bb55f0d76514e3182adcef3ac7a2a2f\",\"defaultNamespace\":\"http:\\/namespace\\/example\\/\",\"name\":\"example\",\"globalGraphID\":\"467257310adf4aeb98bd2bd4a83be86e\"}")})),
            @ApiResponse(code = 400, message = "body is missing")})
    @POST @Path("globalGraph/")
    @Consumes("text/plain")
    public Response POST_globalGraph(
            @ApiParam(value = "json object2 with global graph information", required = true,example ="{\"name\":\"example\",\"defaultNamespace\":\"http:/namespace/example/\"}")
            String body) {

        LOGGER.info("[POST /globalGraph/] body = "+body);
        validator.validateGeneralBody(body,"POST /globalGraph");
        JSONObject objBody = (JSONObject) JSONValue.parse(body);

        globalGraphR.create(objBody);

        return Response.ok(objBody.toJSONString()).build();
    }


    @ApiOperation(value = "Create a new global graph from a OWL file",produces = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "body is missing")})
    @POST @Path("globalGraph/import")
    @Consumes("text/plain")
    public Response POST_importGlobalGraph(
            @ApiParam(value = "json object2 with global graph information", required = true)
                    String body) {

        LOGGER.info("[POST /globalGraph/import] body = "+body);
        validator.validateGeneralBody(body,"POST /globalGraph");
        JSONObject objBody = (JSONObject) JSONValue.parse(body);

        ImportOWLtoGlobalGraph parser = new ImportOWLtoGlobalGraph();
        parser.convert(objBody.getAsString("path"), objBody.getAsString("name"));
        return Response.ok(objBody.toJSONString()).build();
    }

    @ApiOperation(value = "Save a triple for a given named graph",consumes = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK", examples = @Example(value = {@ExampleProperty(value = "{\"namedGraph\":\"http:\\/namespace\\/example\\/8bb55f0d76514e3182adcef3ac7a2a2f\",\"defaultNamespace\":\"http:\\/namespace\\/example\\/\",\"name\":\"example\",\"globalGraphID\":\"467257310adf4aeb98bd2bd4a83be86e\"}")})),
            @ApiResponse(code = 400, message = "triples are missing")})
    @POST @Path("globalGraph/{namedGraph}/triple/")
    @Consumes("text/plain")
    public Response POST_triple(@PathParam("namedGraph") String namedGraph, String body) {
        LOGGER.info("[POST /globalGraph/"+namedGraph+"/triple] body = "+body);
        validator.validateBodyTriples(body,"POST /globalGraph/"+namedGraph+"/triple");
        JSONObject objBody = (JSONObject) JSONValue.parse(body);
        graphO.addTriple(namedGraph,objBody);
        return Response.ok().build();
    }

    @ApiOperation(value = "Save and update graph in turtle format",consumes = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "body is missing")})
    @POST @Path("globalGraph/{namedGraph}/TTL")
    @Consumes("text/plain")
    public Response POST_TTL(@PathParam("namedGraph") String namedGraph, String body) {
        LOGGER.info("[POST /globalGraph/"+namedGraph+"/ ttl] body = "+body);
        validator.validateGeneralBody(body,"POST /globalGraph/"+namedGraph+"/triple");

        JSONObject objBody = (JSONObject) JSONValue.parse(body);
        JSONObject objMod = (JSONObject) objBody.get("modified");
        if(objMod.getAsString("isModified").equals("true")){
            globalService.updateTriples(objMod,namedGraph);
        }else{
            graphO.loadTTL(namedGraph,objBody.getAsString("ttl"));
        }
        return Response.ok().build();
    }

    @ApiOperation(value = "Save graphical graph",consumes = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 400, message = "graph cannot be empty")})
    @POST @Path("globalGraph/{globalGraphID}/graphicalGraph")
    @Consumes("text/plain")
    public Response POST_graphicalGraph(@PathParam("globalGraphID") String globalGraphID, String body) {
        LOGGER.info("[POST /globalGraph/"+globalGraphID+"/graphicalGraph");
        validator.validateGraphicalGraphBody(body,"POST /globalGraph/"+globalGraphID+"/graphicalGraph");
        globalGraphR.updateByGlobalGraphID(globalGraphID, GlobalGraphMongo.FIELD_graphicalGraph.val(),body);

        return Response.ok().build();
    }

    @ApiOperation(value = "Delete a node from the global graph",consumes = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 409, message = "node cannot be deleted")})
    @DELETE @Path("globalGraph/{namedGraph}/node")
    @Consumes("text/plain")
    public Response DELETE_nodeGlobalGraph(@PathParam("namedGraph") String namedGraph, String body) {
        LOGGER.info("[DELETE /globalGraph/ "+namedGraph+" /node");

        JSONObject objBody = (JSONObject) JSONValue.parse(body);
        globalService.deleteNode(namedGraph,objBody.getAsString("iri"));
        return Response.ok().build();
    }

    @ApiOperation(value = "Delete a property from the global graph",consumes = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK"),
            @ApiResponse(code = 409, message = "node cannot be deleted")})
    @DELETE @Path("globalGraph/{namedGraph}/property")
    @Consumes("text/plain")
    public Response DELETE_propertyGlobalGraph(@PathParam("namedGraph") String namedGraph, String body) {
        LOGGER.info("[DELETE /globalGraph/ "+namedGraph+" /property");


        JSONObject objBody = (JSONObject) JSONValue.parse(body);
        globalService.deleteProperty(namedGraph,objBody.getAsString("sIRI"),objBody.getAsString("pIRI"),objBody.getAsString("oIRI"));
        return Response.ok().build();
    }

    @ApiOperation(value = "Delete a global graph and its related LAVMappings, if exist",consumes = MediaType.TEXT_PLAIN)
    @ApiResponses(value ={
            @ApiResponse(code = 200, message = "OK")})
    @DELETE @Path("globalGraph/{globalGraphID}")
    @Consumes("text/plain")
    public Response DELETE_GlobalGraph(@PathParam("globalGraphID") String globalGraphID) {
        LOGGER.info("[DELETE /globalGraph/ "+globalGraphID);


        globalService.deleteGlobalGraph(globalGraphID);
        return Response.ok().build();
    }



/*
    @POST
    @Path("globalGraph/sparQLQuery")
    @Consumes(MediaType.TEXT_PLAIN)
    public Response POST_BDI_ontology_SparQLQuery(String body) {
        LOGGER.info("Query: " + body);
        return Response.ok(new Gson().toJson("SparQL Query")).build();
    }
*/
}
