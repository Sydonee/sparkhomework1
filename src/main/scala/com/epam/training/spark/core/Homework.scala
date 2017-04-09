package com.epam.training.spark.core

import java.time.LocalDate

import com.epam.training.spark.core.domain.Climate
import com.epam.training.spark.core.domain.MaybeDoubleType.MaybeDouble
import com.epam.training.spark.core.domain.PrecipitationType.Precipitation
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Homework {
  val DELIMITER = ";"
  val RAW_BUDAPEST_DATA = "data/budapest_daily_1901-2010.csv"
  val OUTPUT_DUR = "output"

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("EPAM BigData training Spark Core homework")
      .setIfMissing("spark.master", "local[2]")
      .setIfMissing("spark.sql.shuffle.partitions", "10")
    val sc = new SparkContext(sparkConf)

    processData(sc)

    sc.stop()

  }

  def processData(sc: SparkContext): Unit = {

    /**
      * Task 1
      * Read raw data from provided file, remove header, split rows by delimiter
      */
    val rawData: RDD[List[String]] = getRawDataWithoutHeader(sc, Homework.RAW_BUDAPEST_DATA)

    /**
      * Task 2
      * Find errors or missing values in the data
      */
    val errors: List[Int] = findErrors(rawData)
    println(errors)

    /**
      * Task 3
      * Map raw data to Climate type
      */
    val climateRdd: RDD[Climate] = mapToClimate(rawData)

    /**
      * Task 4
      * List average temperature for a given day in every year
      */
    val averageTemeperatureRdd: RDD[Double] = averageTemperature(climateRdd, 1, 2)

    /**
      * Task 5
      * Predict temperature based on mean temperature for every year including 1 day before and after
      * For the given month 1 and day 2 (2nd January) include days 1st January and 3rd January in the calculation
      */
    val predictedTemperature: Double = predictTemperature(climateRdd, 1, 2)
    println(s"Predicted temperature: $predictedTemperature")

  }

  def getRawDataWithoutHeader(sc: SparkContext, rawDataPath: String): RDD[List[String]] = {
    val csv = sc.textFile(rawDataPath).cache()

    val splittedLines = csv.filter(!_.startsWith("#"))
      .map(_.split(";", 7))

    return splittedLines.map(x=>x.toList)
  }

  def findErrors(rawData: RDD[List[String]]): List[Int] = {
    val onlyData = rawData.collect()
    val retList=new Array[Int](onlyData.map(x=>x.length).max)
    retList.indices.foreach(i => retList(i)=onlyData.count(l=>l(i)==""))

    return retList.toList
  }

  def mapToClimate(rawData: RDD[List[String]]): RDD[Climate] = {
    return rawData.map(x=>new Climate(LocalDate.parse(x(0)), MaybeDouble.apply(x(1)),MaybeDouble.apply(x(2))
      ,MaybeDouble.apply(x(3)),MaybeDouble.apply(x(4)),Precipitation.apply(x(5)),MaybeDouble.apply(x(6))))
  }

  def averageTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): RDD[Double] = {
    val relevantRecords=climateData.filter(d=>d.observationDate.getMonthValue()==month && d.observationDate.getDayOfMonth()==dayOfMonth)
    return relevantRecords.map(x=>  x.meanTemperature.value)
  }

  def predictTemperature(climateData: RDD[Climate], month: Int, dayOfMonth: Int): Double = {
    val relevantRecords = climateData.filter(x=>
      x.observationDate.getMonthValue==month &&
        (x.observationDate.getDayOfMonth==dayOfMonth ||
          x.observationDate.getDayOfMonth==dayOfMonth-1 ||
          x.observationDate.getDayOfMonth==dayOfMonth+1))
    val sumOfTemps = relevantRecords.map(x=>  x.meanTemperature.value).sum()
    return sumOfTemps/relevantRecords.collect().length
  }


}


