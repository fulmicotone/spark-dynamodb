package com.fulmicotone.dynamodb.read.business;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fulmicotone.dynamodb.common.DDBJobConf;
import com.fulmicotone.dynamodb.read.exception.DDBDeserializationException;
import com.fulmicotone.dynamodb.read.function.impl.DDBItemMapToRow;
import com.fulmicotone.dynamodb.read.function.IDDBDeserializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Map;

/**
 * class to extends during the reading process from dynamo
 * it allows to convert Function<Tuple2<Text, DynamoDBItemWritable> ddbRow.get to Spark Row
 */
public abstract class DDBDeserializer implements IDDBDeserializer,Serializable {

    @Override
    public Row deserialize(Map<String,AttributeValue> itemDDB)  {

        try {
            return new DDBItemMapToRow().deserialize(itemDDB,structType());
        } catch (DDBDeserializationException e) {

            e.printStackTrace();

            return null;
        }
    }

    public abstract StructType structType();




}
