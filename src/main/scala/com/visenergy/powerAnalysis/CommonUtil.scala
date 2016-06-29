package com.visenergy.powerAnalysis

import java.math.BigDecimal
import org.apache.spark.mllib.linalg.Vector

object CommonUtil {
	/*
	 * 保留deimal小数
	 */
	def reserveDecimals(destDouble:Double,decimal:Int) : Double = {
	  val bg = new BigDecimal(destDouble)
	  bg.setScale(decimal, BigDecimal.ROUND_HALF_UP).doubleValue()
	}
	
	/*
	 * 向量比较
	 */
	def vectorEquals(vec1:Vector,vec2:Vector) : Boolean = {
	  vec1.toJson.equals(vec2.toJson)
	}
}