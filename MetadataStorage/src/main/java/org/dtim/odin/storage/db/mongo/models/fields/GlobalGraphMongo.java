package org.dtim.odin.storage.db.mongo.models.fields;

public enum GlobalGraphMongo {

    FIELD_graphicalGraph("graphicalGraph"),
    FIELD_wrappers("wrappers"),
    FIELD_GlobalGraphID("globalGraphID"),
    FIELD_NamedGraph("namedGraph");


    private String element;

    GlobalGraphMongo(String element) {
        this.element = element;
    }

    public String val() {
        return element;
    }
}
