package eu.supersede.mdm.storage.db.mongo.models.fields;

public enum IntegratedDataSourceMongo {

    FIELD_dataSourceID("dataSourceID");

    private String element;

    IntegratedDataSourceMongo(String element) {
        this.element = element;
    }

    public String val() {
        return element;
    }
}

