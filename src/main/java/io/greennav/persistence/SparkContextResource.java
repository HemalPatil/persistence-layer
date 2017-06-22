//package io.greennav.persistence;
//
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.SparkSession;
//
///**
// * Created by Hemal on 15-Jun-17.
// */
//public class SparkContextResource
//{
//	private static SparkSession session = null;
//	private static SQLContext sqlContext = null;
//
//	private static void initializeResource()
//	{
//		if(session == null)
//		{
//			session = SparkSession.builder()
//					.appName("io.greennav.persistence.DatabaseApplication")
//					.master("local")
//					.getOrCreate();
//		}
//		if(sqlContext == null)
//		{
//			sqlContext = session.sqlContext();
//		}
//	}
//
//	public static SQLContext getSQLContext()
//	{
//		initializeResource();
//		return sqlContext;
//	}
//}
