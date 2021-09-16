package com.sql.transform;

public interface ITranformToEquivalent {
    Boolean canTransform(String query);
    String transform(String query);
}

