package com.fulmicotone.dynamodb.write.function;

import com.fulmicotone.dynamodb.common.DDBJobConf;
import com.fulmicotone.dynamodb.write.exception.DDBWriteException;
import com.fulmicotone.dynamodb.write.function.impl.DDBSerializer;
import org.apache.spark.sql.Dataset;

@FunctionalInterface
public interface IDatasetDDBWriter<A extends DDBSerializer,B extends DDBJobConf,C extends Dataset> {

    void write(A serializer, B ddbJobConf, C inputDataset) throws DDBWriteException;}