package eu.supersede.mdm.storage.db.jena;

import eu.supersede.mdm.storage.db.jena.query.SelectQuery;
import eu.supersede.mdm.storage.model.Namespaces;
import eu.supersede.mdm.storage.model.metamodel.GlobalGraph;
import eu.supersede.mdm.storage.util.RDFUtil;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.jena.query.*;
import org.apache.jena.system.Txn;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

public class GraphOperations {

    @Inject
    SelectQuery selectQ;


    private Dataset ds;

    @PostConstruct
    public void init() {
        ds = JenaConnection.getInstance().getTDBDataset();
    }

    public JSONArray getFeaturesFromGraph(String namedGraph){
        JSONArray features = new JSONArray();

        Query SPARQL = selectQ.selectAllSubjectsFromGraph(namedGraph,Namespaces.rdf.val()+"type",GlobalGraph.FEATURE.val());

        runAQuery(SPARQL).forEachRemaining(t -> {
            features.add(t.get("s").asNode().getURI());
        });
        return features;
    }

    public JSONObject getFeaturesWithConceptFromGraph(String namedGraph){
        JSONObject featureConcept = new JSONObject();

        Query SPARQL = selectQ.selectSubjectAndFeatureFromGraph(namedGraph,GlobalGraph.HAS_FEATURE.val());

        runAQuery(SPARQL).forEachRemaining(t -> {
            featureConcept.put(t.get("o").asNode().getURI(), t.get("s").asNode().getURI());
        });
        return featureConcept;
    }


    public ResultSet runAQuery(Query query) {

        ResultSet resultSet = Txn.calculateRead(ds, ()-> {
            try(QueryExecution qExec = QueryExecutionFactory.create(query, ds)) {
                return ResultSetFactory.copyResults(qExec.execSelect()) ;
            }
        }) ;
        return resultSet;
    }
}
