package com.fulmicotone.dynamodb.read.function;

import com.fulmicotone.dynamodb.common.DDBJobConf;
import com.fulmicotone.dynamodb.read.business.DDBDeserializer;
import com.fulmicotone.dynamodb.read.exception.DDBReadException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface IDatasetDDBReader<A extends DDBDeserializer, B extends DDBJobConf,C extends SparkSession, R extends Dataset<Row>> {

    R read(A deserializer,B ddbJobConf,C sparkSession) throws DDBReadException;

}