package eu.supersede.mdm.storage.db.mongo.models.fields;

public enum WrapperMongo {

    FIELD_IRI("iri"),
    FIELD_wrapperID("wrapperID"),
    FIELD_attributesName("attributes.name");

    private String element;

    WrapperMongo(String element) {
        this.element = element;
    }

    public String val() {
        return element;
    }
}
