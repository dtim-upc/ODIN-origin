package org.dtim.odin.storage.resources;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import net.minidev.json.JSONObject;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.parsers.OWLtoD3;
import scala.Tuple3;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Created by snadal on 17/05/16.
 */
@Path("metadataStorage")
public class GraphResource {

    GraphOperations graphO = new GraphOperations();
    /**
     * Get the content of the artifact, i.e. the triples
     */
    @GET @Path("graph/{iri}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_graph(@PathParam("iri") String iri) {
        System.out.println("[GET /graph/"+iri);
        String out = "";

        try{
            out = graphO.selectTriplesFromGraphAsXML(iri);
        } catch (Exception e) {
            e.printStackTrace();
            return Response.ok("Error: "+e.toString()).build();
        }

        JSONObject res = new JSONObject();
        res.put("rdf",out);
        return Response.ok(res.toJSONString()).build();
    }

    /**
     * Get the graphical representation of the graph
     */
    @GET @Path("graph/{artifactType}/{iri}/graphical")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response GET_artifact_content_graphical(@PathParam("artifactType") String artifactType, @PathParam("iri") String iri) {
        System.out.println("[GET /graph/"+artifactType+"/"+iri+"/graphical");
        List<Tuple3<Resource,Property,Resource>> triples = Lists.newArrayList();
        graphO.runAQuery("SELECT * WHERE { GRAPH <"+iri+"> {?s ?p ?o} }").forEachRemaining(triple -> {
            triples.add(new Tuple3<>(new ResourceImpl(triple.get("s").toString()),
                    new PropertyImpl(triple.get("p").toString()),new ResourceImpl(triple.get("o").toString())));
        });
        String JSON = OWLtoD3.parse(artifactType, triples);
        return Response.ok((JSON)).build();
    }


    @POST @Path("graph/{iri}")
    @Consumes("text/plain")
    public Response POST_graph(@PathParam("iri") String iri, String RDF) {
        System.out.println("[POST /graph/"+iri);
        /* Store RDF into a temporal file */
        String tempFileName = UUID.randomUUID().toString();
        String filePath = "";
        try {
            File tempFile = File.createTempFile(tempFileName,".tmp");
            filePath = tempFile.getAbsolutePath();
            Files.write(RDF.getBytes(),tempFile);
        } catch (IOException e) {
            e.printStackTrace();
            return Response.ok("Error: "+e.toString()).build();
        }
        graphO.createGraphWithOntModel(iri,filePath);
        return Response.ok().build();
    }

    @POST @Path("graph/{iri}/triple/{s}/{p}/{o}")
    @Consumes("text/plain")
    public Response POST_triple(@PathParam("iri") String iri, @PathParam("s") String s, @PathParam("p") String p, @PathParam("o") String o) {
        System.out.println("[POST /graph/"+iri+"/triple");
        graphO.addTriple(iri,s,p,o);
        return Response.ok().build();
    }

    /*
    @POST @Path("artifacts/{graph}/graphicalGraph")
    @Consumes("text/plain")
    public Response POST_graphicalGraph(@PathParam("graph") String graph, String body) {
        System.out.println("[POST /artifacts/"+graph+"/graphicalGraph");

        MongoClient client = Utils.getMongoDBClient();
        MongoCollection<Document> artifacts = getArtifactsCollection(client);

        artifacts.findOneAndUpdate(
                new Document().append("graph",graph),
                new Document().append("$set", new Document().append("graphicalGraph",body))
        );

        client.close();

        return Response.ok().build();
    }
    */

}