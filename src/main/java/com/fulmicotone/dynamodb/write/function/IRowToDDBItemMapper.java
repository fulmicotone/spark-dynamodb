package com.fulmicotone.dynamodb.write.function;

import com.fulmicotone.dynamodb.common.DDBField;
import com.fulmicotone.dynamodb.write.exception.DDBWritableItemCreationException;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.spark.sql.Row;

import java.util.List;

@FunctionalInterface
public interface IRowToDDBItemMapper<E extends Row,T extends List<DDBField>,R extends DynamoDBItemWritable>
{  R serialize(E row,T ddbFields) throws DDBWritableItemCreationException;}