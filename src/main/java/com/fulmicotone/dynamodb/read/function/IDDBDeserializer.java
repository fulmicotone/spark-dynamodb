package com.fulmicotone.dynamodb.read.function;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fulmicotone.dynamodb.common.DDBJobConf;
import com.fulmicotone.dynamodb.read.exception.DDBDeserializationException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public interface   IDDBDeserializer {

     Row deserialize(Map<String, AttributeValue> mapDDBItem) throws DDBDeserializationException;

     StructType structType();



}
