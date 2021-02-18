package org.dtim.odin.storage.bdi.extraction;

import net.minidev.json.JSONObject;
import org.apache.jena.ontology.ObjectProperty;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.dtim.odin.storage.bdi.extraction.metamodel.NewSourceLevel2;
import org.dtim.odin.storage.util.JSONUtil;

public class JSONAdapter {
	private final OntClass ontClass;
	private final ObjectProperty parentLink;
	

	public JSONAdapter(OntModel model, JSONObject json, String key) {
		if (JSONUtil.isObject(json.get(key))) {
			//ontClass = NewSourceLevel2.RDFSClass.asOntClass(model);
			//parentLink = NewSourceLevel2.RDFSSubClassOf.asObjectProperty(model);
			ontClass = NewSourceLevel2.RDFProperty.asOntClass(model);
			parentLink = NewSourceLevel2.RDFSDomain.asObjectProperty(model);
			

		} else if (JSONUtil.isArray(json.get(key))) {
			ontClass = NewSourceLevel2.RDFProperty.asOntClass(model); //RDFSeq  //class attributes with arrays
			parentLink = NewSourceLevel2.RDFSDomain.asObjectProperty(model); //RDFSMember instead of RDFSDomain

		} else {
			ontClass = NewSourceLevel2.RDFProperty.asOntClass(model);
			parentLink = NewSourceLevel2
					.RDFSDomain.asObjectProperty(model);
		}
	}
	
	
	public OntClass getOntClass() {
		return ontClass;
	}

	public ObjectProperty getParentLink() {
		return parentLink;
	}
}
