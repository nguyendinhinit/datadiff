package vn.bnh.datadiff.services;

import vn.bnh.datadiff.dto.DBObject;

public interface QueryBuilderService {
    public String buildQuery(DBObject dbObject, String type);
}
