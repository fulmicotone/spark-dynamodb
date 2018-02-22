package com.fulmicotone.dynamodb.common;

import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.sql.SparkSession;

import java.util.Optional;

public   class DDBJobConf extends JobConf  {

    private final static String input_format_class_label ="mapred.input.format.class";
    private final static String output_format_class_label ="mapred.output.format.class";
    private final static String output_table_name_label ="dynamodb.output.tableName";
    private final static String input_table_name_label ="dynamodb.input.tableName";
    private final static String service_name_label ="dynamodb.servicename";
    private final static String ddb_endpoint_label ="dynamodb.endpoint";
    private final static String ddb_region_label ="dynamodb.regionid";
    private final static String ddb_version_label ="dynamodb.version";
    private final static String dynamo_throughput_read_label ="dynamodb.throughput.read";
    private final static String dynamo_throughput_read_percent_label ="dynamodb.throughput.read.percent";
    private final static String dynamo_throughput_write_percent_label ="dynamodb.throughput.write.percent";
    private final static String dynamo_throughput_write_label ="dynamodb.throughput.write";

    private static String dynamo_input_class_value="org.apache.hadoop.dynamodb.read.DynamoDBInputFormat";
    private static String dynamo_output_class_value ="org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat";
    private static String dynamo_service_name_value ="dynamodb";
    private static String dynamo_endpoint_value ="dynamodb.us-east-1.amazonaws.com";
    private static String dynamo_region_value ="us-east-1";
    private static String dynamo_version_value="2011-12-05";



    private DDBJobConf(
             String serviceName,
             String ddbEndpoint,
             String ddbRegion,
             String version,
             String inputTable,
             String outputTableName,
             float writePercent,
             int writeThroughput,
             float readPercent,
             int readThroughput,
             String inputClass,
             String outputClass) {



        if(Optional.ofNullable(serviceName).isPresent()){this.set(service_name_label, serviceName);}
        if(Optional.ofNullable(ddbEndpoint).isPresent()){this.set(ddb_endpoint_label, ddbEndpoint);}
        if(Optional.ofNullable(ddbRegion).isPresent()){this.set(ddb_region_label, ddbRegion);}
        if(Optional.ofNullable(version).isPresent()){this.set(ddb_version_label, version);}
        if(Optional.ofNullable(outputTableName).isPresent()){this.set(output_table_name_label, outputTableName);}
        if(Optional.ofNullable(inputTable).isPresent()){this.set(input_table_name_label, inputTable);}
        if(Optional.ofNullable(writePercent).isPresent()){this.set(dynamo_throughput_write_percent_label, writePercent+"");}
        if(Optional.ofNullable(writeThroughput).isPresent()){this.set(dynamo_throughput_write_label, writeThroughput+"");}
        if(Optional.ofNullable(readPercent).isPresent()){this.set(dynamo_throughput_read_percent_label, readPercent+"");}
        if(Optional.ofNullable(readThroughput).isPresent()){this.set(dynamo_throughput_read_label, readThroughput+"");}
        this.set(input_format_class_label, inputClass);
        this.set(output_format_class_label, outputClass);
    }





    public static class Builder{


        private Builder(){}

        private String serviceName=dynamo_service_name_value;
        private String ddbEndpoint=dynamo_endpoint_value;
        private String ddbRegion=dynamo_region_value;
        private String ddbVersion=dynamo_version_value;
        private String inputTable;
        private String outputTableName;
        private float writePercent;
        private int writeThroughput;
        private float readPercent;
        private int readThroughput;
        private String inputClass=dynamo_input_class_value;
        private String outputClass=dynamo_output_class_value;


        public Builder setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder setDdbEndpoint(String ddbEndpoint) {
            this.ddbEndpoint = ddbEndpoint;
            return this;
        }

        public Builder setDdbRegion(String ddbRegion) {
            this.ddbRegion = ddbRegion;
            return this;
        }

        public Builder setInputTable(String inputTable) {
            this.inputTable = inputTable;
            return this;
        }

        public Builder setOutputTableName(String outputTableName) {
            this.outputTableName = outputTableName;
            return this;
        }

        public Builder setWritePercent(float writePercent) {
            this.writePercent = writePercent;
            return this;
        }

        public Builder setWriteThroughput(int writeThroughput) {
            this.writeThroughput = writeThroughput;
            return this;
        }

        public Builder setReadPercent(float readPercent) {
            this.readPercent = readPercent;
            return this;
        }

        public Builder setReadThroughput(int readThroughput) {
            this.readThroughput = readThroughput;
            return this;
        }

        public Builder setInputClass(String inputClass) {
            this.inputClass = inputClass;
            return this;
        }

        public Builder setOutputClass(String outputClass) {
            this.outputClass = outputClass;
            return this;
        }





       public static Builder  newInstance(){
            return new Builder() ;
        }

        public DDBJobConf create(){


            return new DDBJobConf(   serviceName,
              ddbEndpoint,
              ddbRegion,
              ddbVersion,
              inputTable,
              outputTableName,
              writePercent,
              writeThroughput,
              readPercent,
              readThroughput,
              inputClass,
              outputClass );
        }

    }
}
