package com.fulmicotone.dynamodb.common;

import org.apache.spark.sql.SparkSession;
import scala.Option;

import java.util.function.Function;


public class UtilsFunction {

    public static Function<SparkSession,Integer> getParallelismFn=(session)->{

        int defaultParallelism=2;
        Option<String> opt;
        if((opt=session.conf().getOption("spark.default.parallelism")).isEmpty()==false) {
            defaultParallelism = Integer.parseInt(opt.get());
        }
        return defaultParallelism;
    };





}
