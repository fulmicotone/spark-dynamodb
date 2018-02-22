package com.fulmicotone.dynamodb.read.function;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fulmicotone.dynamodb.read.exception.DDBDeserializationException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

@FunctionalInterface
public interface IDDBItemMapToSparkRow<M extends Map<String,AttributeValue>,S,R extends Row> {

     R deserialize (M ddbItemMap,S structType)throws DDBDeserializationException;

}
