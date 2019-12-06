package eu.supersede.mdm.storage.db.mongo.models.fields;

public enum LAVMappingMongo {

    FIELD_LAVMappingID("LAVMappingID"),
    FIELD_graphicalSubGraph("graphicalSubGraph"),
    FIELD_wrapperID("wrapperID"),
    FIELD_globalGraphID("globalGraphID"),


    FIELD_sameAsFeature("sameAs.feature"),
    FIELD_sameAsFeatureUpdate("sameAs.$.feature");

    private String element;

    LAVMappingMongo(String element) {
        this.element = element;
    }

    public String val() {
        return element;
    }
}
