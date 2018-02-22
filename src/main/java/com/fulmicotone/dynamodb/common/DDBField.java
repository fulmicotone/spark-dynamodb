package com.fulmicotone.dynamodb.common;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public   class DDBField<T extends DataType> {

    public final String fieldName;
    public final Class<DataTypes> typeClazz;

    public DDBField(String fieldName, Class<DataTypes> typeClazz){

        this.fieldName=fieldName;
        this.typeClazz=typeClazz;
    }
    }





