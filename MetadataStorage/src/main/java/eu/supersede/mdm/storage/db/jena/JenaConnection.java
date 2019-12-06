package eu.supersede.mdm.storage.db.jena;

import eu.supersede.mdm.storage.util.ConfigManager;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb.TDBFactory;

import java.util.logging.Logger;

public class JenaConnection {

    private static JenaConnection instance = new JenaConnection();
    private static final Logger LOGGER = Logger.getLogger(JenaConnection.class.getName());
    private Dataset dataset;

    public Dataset getTDBDataset() {
        if (dataset == null) {
            try {
                dataset = TDBFactory.createDataset(ConfigManager.getProperty("metadata_db_path") + "/" +
                        ConfigManager.getProperty("metadata_db_name"));
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("An error has occurred obtaining TDB dataset");
            }


        }
        return dataset;
    }

    public void init() {
        getTDBDataset();
    }

    public void close() {
        dataset.close();
        dataset = null;
    }

    public static JenaConnection getInstance() {
        return instance;
    }
}
