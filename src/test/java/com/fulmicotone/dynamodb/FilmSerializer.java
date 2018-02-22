package com.fulmicotone.dynamodb;

import com.fulmicotone.dynamodb.common.DDBField;
import com.fulmicotone.dynamodb.write.function.impl.DDBSerializer;
import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;


import java.util.Arrays;
import java.util.List;

public class FilmSerializer extends DDBSerializer {
    @Override
    protected List<DDBField> fieldMap() {
          return Arrays.asList(
                new DDBField("Film", StringType.class),
                new DDBField("Genre", StringType.class),
                  new DDBField("Lead Studio", StringType.class),
                  new DDBField("Audience score %", IntegerType.class),
                  new DDBField("Profitability", DecimalType.class),
                  new DDBField("Rotten Tomatoes %", StringType.class),
                  new DDBField("Worldwide Gross", StringType.class),
                  new DDBField("Year", IntegerType.class),
                  new DDBField("Actors", StringType.class)

                  );

    }
}
