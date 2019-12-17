package org.dtim.odin.storage.resources;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.dtim.odin.storage.db.jena.GraphOperations;
import org.dtim.odin.storage.db.jena.JenaConnection;
import org.dtim.odin.storage.db.mongo.models.DataSourceModel;
import org.dtim.odin.storage.db.mongo.models.WrapperModel;
import org.dtim.odin.storage.db.mongo.models.fields.WrapperMongo;
import org.dtim.odin.storage.db.mongo.repositories.DataSourceRepository;
import org.dtim.odin.storage.db.mongo.repositories.WrapperRepository;
import org.dtim.odin.storage.model.omq.ConjunctiveQuery;
import org.dtim.odin.storage.model.omq.QueryRewriting_EdgeBased;
import org.dtim.odin.storage.model.omq.relational_operators.Wrapper;
import org.dtim.odin.storage.util.SQLiteUtils;
import scala.Tuple3;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by snadal on 22/11/16.
 */
@Path("metadataStorage")
public class OMQResource {

    WrapperRepository wrapperR = new WrapperRepository();

    DataSourceRepository dataSourceR = new DataSourceRepository();

    GraphOperations graphO = new GraphOperations();

    @POST @Path("omq/fromGraphicalToSPARQL")
    @Consumes("text/plain")
    public Response POST_omq_fromGraphicalToSPARQL(String body) {
        System.out.println("[POST /omq/fromGraphicalToSPARQL/] body = "+body);
        JSONObject objBody = (JSONObject) JSONValue.parse(body);
        String select = "SELECT ";
        String values = "VALUES (";
        String constants = "{(";

        JSONArray projectedFeatures =(JSONArray)objBody.get("projectedFeatures");
        for (int i = 0; i < projectedFeatures.size(); ++i) {
            select += "?v"+(i+1)+" ";
            values += "?v"+(i+1)+" ";
            constants += "<"+projectedFeatures.get(i)+"> ";
        }

        values = values.substring(0,values.length()-1)+")";
        constants = constants.substring(0,constants.length()-1)+")}";

        String pattern = "";
        for (Object selectionElement : ((JSONArray)objBody.get("selection"))) {
            JSONObject selectedElement = (JSONObject)selectionElement;
            if (selectedElement.containsKey("source")) {
                JSONObject source = (JSONObject)selectedElement.get("source");
                JSONObject target = (JSONObject)selectedElement.get("target");
                pattern += "<"+source.getAsString("iri") + "> <" + selectedElement.getAsString("iri") + "> <" +
                        target.getAsString("iri") + "> .\n";
            }
        }
        pattern = pattern.substring(0,pattern.length()-2)+"\n";

        JSONObject out = new JSONObject();
        out.put("sparql",select+"\nWHERE {\n"+values+" "+constants+"\n"+pattern+"}");
        return Response.ok(out.toJSONString()).build();
    }


    @POST @Path("omq/fromSPARQLToRA")
    @Consumes("text/plain")
    public Response POST_omq_fromSPARQLToRA(String body) throws Exception{
        System.out.println("[POST /omq/fromSPARQLToRA/] body = "+body);
        JSONObject objBody = (JSONObject) JSONValue.parse(body);

        String SPARQL = objBody.getAsString("sparql");
        List<String> listOfFeatures = (((JSONArray)JSONValue.parse(objBody.getAsString("features"))).stream().map(o ->String.valueOf(o)).collect(Collectors.toList()));

        //String namedGraph = objBody.getAsString("namedGraph");
        //QueryRewriting qr = new QueryRewriting_DAG(SPARQL.replace("\n"," "));

        Dataset T = JenaConnection.getInstance().getTDBDataset();
        T.begin(ReadWrite.READ);
        Set<ConjunctiveQuery> UCQ = null;
        try {
            UCQ = QueryRewriting_EdgeBased.rewriteToUnionOfConjunctiveQueries(
                    QueryRewriting_EdgeBased.parseSPARQL(SPARQL.replace("\n", " "), T), T)._2;
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }


        JSONObject out = new JSONObject();
        out.put("ra", graphO.nn(UCQ.stream().map(cq -> cq.toString()).collect(Collectors.joining("\nU\n"))));

        T.abort();
//        T.close();


        HashMap<String,String> wrapperIriToID = Maps.newHashMap(); //used to map wrapper IRIs to IDs
        //Populate data here
        JSONArray wrappers = new JSONArray();
        UCQ.forEach(q -> {
            q.getWrappers().forEach(op -> {
//                String wrapperID = MongoCollections.getWrappersCollection(Utils.getMongoDBClient()).find(
//                           new Document("iri",((Wrapper)op).getWrapper())
//                ).first().getString("wrapperID");

                String wrapperID = wrapperR.findByField(WrapperMongo.FIELD_IRI.val(),((Wrapper)op).getWrapper()).getWrapperID();

                wrapperIriToID.putIfAbsent(((Wrapper)op).getWrapper(),wrapperID);
                wrappers.add(wrapperID);
            });
        });

        //Convert to SQL
        StringBuilder SQL = new StringBuilder();
        UCQ.forEach(q -> {
            StringBuilder select = new StringBuilder("SELECT ");
            StringBuilder from = new StringBuilder(" FROM ");
            StringBuilder where = new StringBuilder(" WHERE ");
            //Sort the projections as they are indicated in the interface
            //First remove duplicates based on the features
            List<String> seenFeatures = Lists.newArrayList();
            List<String> withoutDuplicates = Lists.newArrayList();
            q.getProjections().forEach(proj -> {
                if (!seenFeatures.contains(QueryRewriting_EdgeBased.featuresPerAttribute.get(proj))) {
                    withoutDuplicates.add(proj);
                    seenFeatures.add(QueryRewriting_EdgeBased.featuresPerAttribute.get(proj));
                }
            });
            //Now do the sorting
            List<String> projections = Lists.newArrayList(withoutDuplicates);//Lists.newArrayList(q.getProjections());
            projections.sort(Comparator.comparingInt(s -> listOfFeatures.indexOf(QueryRewriting_EdgeBased.featuresPerAttribute.get(s))));
            projections.forEach(proj -> select.append("\""+graphO.nn(proj).split("/")[graphO.nn(proj).split("/").length-1]+"\""+","));
            q.getWrappers().forEach(w -> from.append(wrapperIriToID.get(w.getWrapper())+","));
            q.getJoinConditions().forEach(j -> where.append(
                    "\""+graphO.nn(j.getLeft_attribute()).split("/")[graphO.nn(j.getLeft_attribute()).split("/").length-1]+"\""+
                            " = "+
                            "\""+graphO.nn(j.getRight_attribute()).split("/")[graphO.nn(j.getRight_attribute()).split("/").length-1]+"\""+
                            " AND "));
            SQL.append(select.substring(0,select.length()-1));
            SQL.append(from.substring(0,from.length()-1));
            if (!where.toString().equals(" WHERE ")) {
                SQL.append(where.substring(0, where.length() - " AND ".length()));
            }
            SQL.append(" UNION ");
        });
        String SQLstr = SQL.substring(0,SQL.length()-" UNION ".length())+";";

        System.out.println(SQLstr);

        out.put("wrappers",wrappers);
        out.put("sql",SQLstr);

        return Response.ok(out.toJSONString()).build();
    }

    @POST @Path("omq/fromSQLToData")
    @Consumes("text/plain")
    public Response POST_omq_fromSQLToData(String body) {
        System.out.println("[POST /omq/fromSQLToData/] body = "+body);
        JSONObject objBody = (JSONObject) JSONValue.parse(body);

        String SQL = objBody.getAsString("sql");
        List<String> features = ((JSONArray)objBody.get("features")).stream().map(f -> f.toString()).collect(Collectors.toList());

//        MongoClient client = Utils.getMongoDBClient();
        // Structure with wrapper obj, wrapper ID and list of attributes
        List<Tuple3<Wrapper,String,List<String>>> wrappers = Lists.newArrayList();
        ((JSONArray)objBody.get("wrappers")).forEach(wID -> {
            String strWrapperID = (String)wID;
            WrapperModel wrapper = wrapperR.findByWrapperID(strWrapperID);
//            Document wrapper = MongoCollections.getWrappersCollection(client).find(new Document("wrapperID",strWrapperID)).first();
            DataSourceModel ds = dataSourceR.findByDataSourceID(wrapper.getDataSourceID());
//            Document ds = MongoCollections.getDataSourcesCollection(client).find(new Document("dataSourceID", wrapper.getString("dataSourceID"))).first();
            List<String> attributes = Lists.newArrayList();

            wrapper.getAttributes().forEach(a -> {
                attributes.add(a.getName());
            });

//            ((List<Document>)wrapper.get("attributes")).forEach(a -> {
//                attributes.add("'"+a.getString("name")+"'");
//            });
            wrappers.add(new Tuple3<>(Wrapper.specializeWrapper(ds,wrapper.getQuery()),strWrapperID,attributes));
        });

        JSONArray data = new JSONArray();

        wrappers.forEach(w -> {
            SQLiteUtils.createTable(w._2(),w._3());
            try {
                w._1().populate(w._2(),w._3());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        System.out.println(SQL);
        SQLiteUtils.executeSelect(SQL,features).forEach(d -> data.add(d));

//        client.close();
        JSONObject out = new JSONObject();
        out.put("data",data);
        return Response.ok(out.toJSONString()).build();
    }

}
