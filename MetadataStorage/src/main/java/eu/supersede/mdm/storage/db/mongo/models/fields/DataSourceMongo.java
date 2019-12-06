package eu.supersede.mdm.storage.db.mongo.models.fields;

public enum DataSourceMongo {

    FIELD_DataSourceID("dataSourceID"),
    FIELD_Wrappers("wrappers");

    private String element;

    DataSourceMongo(String element) {
        this.element = element;
    }

    public String val() {
        return element;
    }
}
