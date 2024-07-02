SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.session.state.builder", "org.apache.spark.sql.hive.UQueryHiveACLSessionStateBuilder")
                .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.UQueryHiveACLExternalCatalog")
                .config("spark.sql.extensions","org.apache.spark.sql.DliSparkExtension")
                .appName("java_spark_demo")
                .getOrCreate();
