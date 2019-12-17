package org.dtim.odin.storage.tests;

import com.google.common.collect.Lists;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.dtim.odin.storage.ApacheMain;
import org.dtim.odin.storage.model.omq.ConjunctiveQuery;
import org.dtim.odin.storage.model.omq.QueryRewriting_SimpleGraph;
import org.dtim.odin.storage.util.Tuple2;
import org.dtim.odin.storage.util.Utils;

import java.util.List;
import java.util.Map;

public class Specialization_Tests {
    private static String basePath = "/home/snadal/UPC/Projects/MDM/";
    public static void main(String[] args) throws Exception {
        ApacheMain.configPath = basePath+"MetadataStorage/config.sergi.properties";
        TestUtils.deleteTDB();
        Map<String,String> prefixes = TestUtils.populatePrefixes(basePath+"datasets/scenarios/SUPERSEDE_Specializations/prefixes.txt");
        TestUtils.populateTriples("http://www.essi.upc.edu/~snadal/Specialization_ontology",basePath+"datasets/scenarios/SUPERSEDE_Specializations/metamodel.txt", prefixes);
        TestUtils.populateTriples("http://www.essi.upc.edu/~snadal/Specialization_ontology",basePath+"datasets/scenarios/SUPERSEDE_Specializations/global_graph.txt", prefixes);
        TestUtils.populateTriples("http://www.essi.upc.edu/~snadal/Specialization_ontology",basePath+"datasets/scenarios/SUPERSEDE_Specializations/source_graph.txt", prefixes);
        TestUtils.populateMappings(basePath+"datasets/scenarios/SUPERSEDE_Specializations/mappings.txt",
                basePath+"datasets/scenarios/SUPERSEDE_Specializations/global_graph.txt", prefixes);
        List<Tuple2<String,String>> queries = TestUtils.getQueries(basePath+"datasets/scenarios/SUPERSEDE_Specializations/queries.txt",prefixes);

        Dataset T = Utils.getTDBDataset(); T.begin(ReadWrite.READ);

        queries.forEach(query -> {
            System.out.println(query._1);
            List<ConjunctiveQuery> CQs = Lists.newArrayList(QueryRewriting_SimpleGraph.rewriteToUnionOfConjunctiveQueries(QueryRewriting_SimpleGraph.parseSPARQL(query._2,T),T));
            for (int i = 0; i < CQs.size(); ++i) {
                System.out.println("    [" + (i + 1) + "/" + (CQs.size()) + "]: " + CQs.get(i));
            }
        });
        TestUtils.deleteTDB();
    }
}
