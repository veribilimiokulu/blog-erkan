package co.veribilimi.spark.breeze.linalg

import breeze.linalg.{*, DenseMatrix, DenseVector}

object BreezeLinalgExamples {
  def main(args: Array[String]): Unit = {
    println("Welcome to Breeze Linalg Examples")


    /****************** DENSE VECTOR   **************************/
    // Create zero vector
    val zeroVector = DenseVector.zeros[Double](5)
    println(zeroVector)

    // Reach vector elements by index
    println("zeroVector 0'ıncı indeks: ",zeroVector(0))

    // Update vector elements by index
    zeroVector(0) = 5.0
    println("zeroVector 0'ıncı indeks: ",zeroVector(0))

    // Create vector from array
    val arrayVector = DenseVector(1,2,3)
    println(arrayVector)

    // Reach vector element by negative index
    println("Eksi 1 indeks: ", arrayVector(-1))

    // Slice zeroVector (assign elements new value and slice it)
    val newZero = zeroVector(3 to 4) := .5
    println("zeroVector: ", zeroVector)
    println("newZero: ",newZero)

    // Assign new values to members and slice
    zeroVector(1 to 2) := DenseVector(.1,.2)
    println(zeroVector)

    /****************** DENSE MATRIX   **************************/

  val zeroMatrix = DenseMatrix.zeros[Int](5,5)
    println(zeroMatrix)

    // Reach matrix elements
    println(zeroMatrix(0,0))

    // Assign new value to Matrix element by index
    zeroMatrix(0,0) = 5
    println(zeroMatrix)

    // Matrix row and col counts
    println(zeroMatrix.rows, zeroMatrix.cols)

    // Get vector from Matrix
    val vectorFromMatrix = zeroMatrix(::,1)
    println("vectorFromMatrix: ", vectorFromMatrix)

    // Assign
    zeroMatrix(4,::) := DenseVector(1,2,3,4,5).t // transpose to match row shape
    println(zeroMatrix)

  zeroMatrix(0 to 1, 0 to 1) := DenseMatrix((3,1),(-1,-2))
    println("zeroMatrix after slice add: \n", zeroMatrix)


    /*************************   OPERATORS   ********************************/

    // Broadcasting

  import breeze.stats.mean

    val dm = DenseMatrix((1.0,2.0,3.0),
                          (4.0,5.0,6.0))
    println("dm: ", dm)

    // add each row element following vector elements
    val res = dm(::, *) + DenseVector(3.0, 4.0)

  println("res: ", res)

    // Add each row the same value
    res(::,*) := DenseVector(3.0, 4.0)
    println("res: ", res)


    //mean of each row
    val meanOfdm = mean(dm(*, ::))
    println("meanOfdm: ", meanOfdm)


    // breeze.stats.distributions
    import breeze.stats.distributions._
    import breeze.stats.meanAndVariance

    val poi = new Poisson(3.0)

    val s = poi.sample(5)
    println("s from poission: ", s)

    val probOfEachs = s.map({
      poi.probabilityOf(_)
    })

    println("Probability of each element of s: ", probOfEachs)

    val doublePoi = for(x <- poi) yield x.toDouble //meanAndVariance requires doubles but Poisson samples over Ints

    println(meanAndVariance(doublePoi.samples.take(1000)))

    println("poi.mean: ", poi.mean, "  poi.variance: ", poi.variance)

  val normalDist = new Gaussian(3.0,3.0)
    val r = normalDist.sample(5)
    println("r from normal distribution", r)
println("mean of r ", mean(r))
    println("normalDist mean: ", normalDist.mean, " normalDist var: ", normalDist.variance,
    "  normalDist std: ", math.sqrt(normalDist.variance))
  }
}
