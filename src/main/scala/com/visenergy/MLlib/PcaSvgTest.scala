package com.visenergy.MLlib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg.DenseMatrix
import breeze.linalg.csvwrite

object PcaSvgTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MovieFeatureTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    
    val path = "test/lfw/*"
    val rdd = sc.wholeTextFiles(path)
    
    val files = rdd.map{ case (fileName,content) =>
      fileName.replace("file:", "")
    }
    
    val pixels = files.map(f => extractPixels(f,50,50))
    
    val vectors = pixels.map(p => Vectors.dense(p))
    //println(pixels.take(10).map(_.take(10).mkString("",",",",...")).mkString("\n"))

    vectors.setName("image-vectors")
    vectors.cache
    
    val scaler = new StandardScaler(withMean = true, withStd = false).fit(vectors)
    val scaledVectors = vectors.map(v => scaler.transform(v))
    //println(scaledVectors.count())
    
    val matrix = new RowMatrix(scaledVectors)
    val K = 10
    val pc = matrix.computePrincipalComponents(K)
    //println(pc)
//    val rows = pc.numRows
//    val cols = pc.numCols
//    println(rows,cols)
//    val pcBreeze = new DenseMatrix(rows,cols,pc.toArray)
//    csvwrite(new File("test/lfw_gray/pc.csv"),pcBreeze)
    //LFW PCA
    val projected = matrix.multiply(pc)
    println(projected.numRows,projected.numCols)
    println(projected.rows.take(5).mkString("\n"))
    
    val svd = matrix.computeSVD(10, computeU=true)
    println(s"U dimension:${svd.U.numRows},${svd.U.numCols}")
    println(s"S dimension:${svd.s.size}")
    println(s"V dimension:${svd.V.numRows},${svd.V.numCols}")
    
    println("1:" + approxEqual(Array(1.0,2.0,3.0),Array(1.0,2.0,3.0)))
    println("2:" + approxEqual(Array(1.0,2.0,3.0),Array(3.0,2.0,1.0)))
    println("3:" + approxEqual(svd.V .toArray,pc.toArray))
    
    val breezeS = breeze.linalg.DenseVector(svd.s.toArray)
    val projectedSVD = svd.U.rows.map{ v =>
      val breezeV = breeze.linalg.DenseVector(v.toArray)
      val multV = breezeV :* breezeS
      Vectors.dense(multV.data)
    }
    
    val num = projected.rows.zip(projectedSVD).map{ case (v1,v2) =>
    	approxEqual(v1.toArray,v2.toArray)
    }.filter(b =>true).count
    println(num)
    
    //评估
//    val sValues = (1 to 5).map{ i => matrix.computeSVD(i, computeU=false).s}
//    sValues.foreach(println)
//    
//    val svd300 = matrix.computeSVD(300,computeU=false)
//    val sMatrix = new DenseMatrix(1,300,svd300.s.toArray)
//    csvwrite(new File("test/lfw_gray/s.csv"),sMatrix)
    //Thread.sleep(6000000)
//    println(pixels.take(10).map(_.take(10).mkString("",",",",...")).mkString("\n"))
//    //将图片进行灰度处理
//    println(files.first)
//    val imagePath = "test/lfw/Aaron_Eckhart/Aaron_Eckhart_0001.jpg"
//    val grayImage = processImage(loadImageFromFile(imagePath),100,100)
//    println(grayImage)
//    ImageIO.write(grayImage, "jpg", new File("test/lfw_gray/Aaron_Eckhart_0002.jpg"))
  }

  def loadImageFromFile(path : String) : BufferedImage = {
    ImageIO.read(new File(path))
  }
  
  def processImage(image : BufferedImage, width : Int, height : Int) : BufferedImage = {
    val bwImage = new BufferedImage(width,height,BufferedImage.TYPE_BYTE_GRAY)
    val g = bwImage.getGraphics()
    g.drawImage(image, 0, 0, width, height, null)
    g.dispose()
    bwImage
  }
  
  def getPixelsFromImage(image : BufferedImage) : Array[Double] = {
    val width = image.getWidth()
    val height = image.getHeight()
    val pixels = Array.ofDim[Double](width * height)
    image.getData().getPixels(0, 0, width, height, pixels)
  }
  
  def extractPixels(path : String,width : Int,height : Int) : Array[Double] = {
    val raw = loadImageFromFile(path)
    val processed = processImage(raw,width,height)
    getPixelsFromImage(processed)
  }
  
  def approxEqual(array1 : Array[Double],array2 :Array[Double],tolerance : Double = 1e-6):Boolean = {
		 val bools = array1.zip(array2).map{ case(v1,v2) => 
		   if(math.abs(math.abs(v1) - math.abs(v2)) > 1e-6) false else true
		 }
		 bools.fold(true)(_&_)
  }
}