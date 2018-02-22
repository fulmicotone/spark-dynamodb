package com.fulmicotone.dynamodb.write;

import com.fulmicotone.dynamodb.common.DDBJobConf;
import com.fulmicotone.dynamodb.write.exception.DDBWriteException;
import com.fulmicotone.dynamodb.write.function.impl.DDBSerializer;
import com.fulmicotone.dynamodb.write.function.IDatasetDDBWriter;
import org.apache.spark.sql.Dataset;

public class DatasetDDBWriter implements IDatasetDDBWriter<DDBSerializer,DDBJobConf,Dataset> {
    @Override
    public void write(DDBSerializer s, DDBJobConf dbconf, Dataset dataset) throws DDBWriteException {
        try {
            dataset.toJavaRDD()
                    .mapToPair(s)
                    .saveAsHadoopDataset(dbconf);
        }catch (Exception ex){ throw  new DDBWriteException(ex.toString()); }

    }

}
