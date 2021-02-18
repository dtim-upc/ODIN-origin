package org.dtim.odin.storage.parsers.models;

public class ClassAttribute {

    String id;
    String iri;
    String baseIri;
    String label;
//    List<Integer> pos; //for now null

    public ClassAttribute(){}

    public ClassAttribute(String id, String iri, String baseIri, String label) {
        this.id = id;
        this.iri = iri;
        this.baseIri = baseIri;
        this.label = label;
    }
}
