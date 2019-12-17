package org.dtim.odin.storage.validator;

import org.apache.commons.lang.StringUtils;
import org.dtim.odin.storage.errorhandling.exception.ValidationException;

public class GlobalGraphValidator {

    public void validateGeneralBody(String body,String location){
        if (StringUtils.isEmpty(body))
            throw new ValidationException("Invalid or missing body",location,"body is missing");
    }

    public void validateGraphicalGraphBody(String body, String location){
        if (StringUtils.isEmpty(body))
            throw new ValidationException("Invalid or missing body",location,"graph cannot be empty");
    }

    public void validateBodyTriples(String body, String location){
        if (StringUtils.isEmpty(body))
            throw new ValidationException("Invalid or missing body",location,"triples are missing");
    }

}
