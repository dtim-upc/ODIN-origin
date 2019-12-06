package eu.supersede.mdm.storage.db.mongo.models;

/**
 * Class represented the object DataSource in Integreted DataSource Model
 */
public class IDSDataSources {

    private String dataSourceID;
    private String alignmentsIRI;
    private String dataSourceName;

    public String getDataSourceID() {
        return dataSourceID;
    }

    public void setDataSourceID(String dataSourceID) {
        this.dataSourceID = dataSourceID;
    }

    public String getAlignmentsIRI() {
        return alignmentsIRI;
    }

    public void setAlignmentsIRI(String alignmentsIRI) {
        this.alignmentsIRI = alignmentsIRI;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }
}
