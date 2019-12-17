package org.dtim.odin.storage.db.mongo.models;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.bson.types.ObjectId;

import java.util.List;

public class IntegratedDataSourcesModel {

    @JsonSerialize(using = ToStringSerializer.class)
    private ObjectId id;
    private String dataSourceID;
    private String parsedFileAddress;
    private String name;
    private String schema_iri;
    private List<IDSDataSources> dataSources;
    private String graphicalGraph;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getDataSourceID() {
        return dataSourceID;
    }

    public void setDataSourceID(String dataSourceID) {
        this.dataSourceID = dataSourceID;
    }

    public String getParsedFileAddress() {
        return parsedFileAddress;
    }

    public void setParsedFileAddress(String parsedFileAddress) {
        this.parsedFileAddress = parsedFileAddress;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchema_iri() {
        return schema_iri;
    }

    public void setSchema_iri(String schema_iri) {
        this.schema_iri = schema_iri;
    }

    public List<IDSDataSources> getDataSources() {
        return dataSources;
    }

    public void setDataSources(List<IDSDataSources> dataSources) {
        this.dataSources = dataSources;
    }

    public String getGraphicalGraph() {
        return graphicalGraph;
    }

    public void setGraphicalGraph(String graphicalGraph) {
        this.graphicalGraph = graphicalGraph;
    }
}
