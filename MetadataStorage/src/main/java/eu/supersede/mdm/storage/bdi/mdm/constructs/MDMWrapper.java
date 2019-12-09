package eu.supersede.mdm.storage.bdi.mdm.constructs;

import eu.supersede.mdm.storage.bdi.extraction.Namespaces;
import eu.supersede.mdm.storage.db.jena.GraphOperations;
import eu.supersede.mdm.storage.db.mongo.models.fields.GlobalGraphMongo;
import eu.supersede.mdm.storage.db.mongo.repositories.GlobalGraphRepository;
import eu.supersede.mdm.storage.resources.bdi.SchemaIntegrationHelper;
import eu.supersede.mdm.storage.service.WrapperService;
import eu.supersede.mdm.storage.util.RDFUtil;
import eu.supersede.mdm.storage.util.Utils;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;

import javax.inject.Inject;

/**
 * Created by Kashif-Rabbani in June 2019
 */
public class MDMWrapper {

    GlobalGraphRepository globalGraphR = new GlobalGraphRepository();

    WrapperService wrapperS = new WrapperService();

    GraphOperations graphO = new GraphOperations();

    private String mdmGgIri;
    private JSONObject globalGraphInfo;
    private JSONObject wrapper = new JSONObject();
    private JSONArray wrappersIds = new JSONArray();
    private final SchemaIntegrationHelper schemaIntegrationHelper = new SchemaIntegrationHelper();
    MDMWrapper(JSONObject ggInfo, String mdmGlobalGraphIri) {
        this.globalGraphInfo = ggInfo;
        mdmGgIri = mdmGlobalGraphIri;
        run();
    }

    private void run() {
        createWrappers();
        addWrappersInfoInGGMongoCollection();
    }

    private void createWrappers() {
        /*Iterate over all data sources of BDI global graph to convert to wrappers*/
        JSONArray dataSourcesArray = (JSONArray) globalGraphInfo.get("dataSources");
        for (Object o : dataSourcesArray) {
            JSONObject dataSource = (JSONObject) o;
            //System.out.println(dataSource.toJSONString());
            populateWrapperContent(dataSource);
            try {
                JSONObject res = wrapperS.createWrapper(wrapper.toJSONString());
                //System.out.println(res.toJSONString());
                wrappersIds.add(res.getAsString("wrapperID"));
                //HttpUtils.sendPost(wrapper, postWrapperUrl);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    private void populateWrapperContent(JSONObject dataSource) {
        String sourceIRI = Namespaces.Schema.val() + dataSource.getAsString("dataSourceName");
        checkNamedGraph(sourceIRI);
        wrapper.put("name", dataSource.getAsString("dataSourceName").replaceAll(" ", "") + "_Wrapper");
        wrapper.put("dataSourceID", dataSource.getAsString("dataSourceID"));

        JSONObject dataSourceInfo = new JSONObject();
        dataSourceInfo = (JSONObject) JSONValue.parse(schemaIntegrationHelper.getDataSourceInfo(dataSource.getAsString("dataSourceID")));

        if (dataSourceInfo.getAsString("type").equals("csv")) {
            wrapper.put("query", "{\"csvColumnDelimiter\":\",\",\"csvRowDelimiter\":\"\\\\n\",\"headersInFirstRow\":true}");
        }

        //TODO Handle the "query" string of all other sources....

        JSONArray attributes = new JSONArray();
        String getProperties = " SELECT * WHERE { GRAPH <" + sourceIRI + "> { ?property rdfs:domain ?domain; rdfs:range ?range . FILTER NOT EXISTS {?range rdf:type rdfs:Class.}} }";
        graphO.runAQuery(graphO.sparqlQueryPrefixes + getProperties).forEachRemaining(triple -> {
            //System.out.print(triple.get("property") + "\t");
            //System.out.print(triple.get("domain") + "\t");
            //System.out.print(triple.get("range") + "\n");

            JSONObject temp = new JSONObject();
            temp.put("isID", "false");
            temp.put("name", triple.getResource("property").getLocalName());
            temp.put("iri", triple.getResource("property").getURI());
            attributes.add(temp);
            //mdmGlobalGraph.add(triple.getResource("property"), new PropertyImpl(RDF.TYPE), new ResourceImpl(GlobalGraph.FEATURE.val()));
        });
        wrapper.put("attributes", attributes);
        //System.out.println(wrapper.toJSONString());
    }

    private void addWrappersInfoInGGMongoCollection() {
        globalGraphR.updateByGlobalGraphID(mdmGgIri, GlobalGraphMongo.FIELD_wrappers.val(),wrappersIds);

    }

    public static void checkNamedGraph(String uri) {
        System.out.printf("Source URI: " + uri);
        Dataset ds = Utils.getTDBDataset();
        ds.begin(ReadWrite.WRITE);
        if (ds.containsNamedModel(uri)) {
            System.out.println("True - Size: " + ds.getNamedModel(uri).size());
            //ds.removeNamedModel(uri);
        } else {
            System.out.println("False");
        }
        ds.commit();
        ds.end();
        ds.close();

    }
}
