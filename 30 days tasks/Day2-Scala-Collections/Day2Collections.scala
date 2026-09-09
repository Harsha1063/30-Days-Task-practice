object Day2Collections {

  def main(args: Array[String]): Unit = {

    // 1. List - map, filter, flatMap, reduce
    val sales = List(100, 250, 150, 300, 200)

    val mappedSales = sales.map(amount => amount * 2)
    println("Mapped Sales: " + mappedSales)

    val filteredSales = sales.filter(amount => amount >= 200)
    println("Filtered Sales: " + filteredSales)

    val flatMappedSales = sales.flatMap(amount => List(amount, amount + 50))
    println("FlatMapped Sales: " + flatMappedSales)

    val totalSales = sales.reduce((a, b) => a + b)
    println("Total Sales: " + totalSales)


    // 2. Vector - indexed customer records
    val customers = Vector(
      "C001 - Ravi",
      "C002 - Suresh",
      "C003 - Priya",
      "C004 - Anjali"
    )

    println("\nCustomer Records:")
    println("Customer at index 0: " + customers(0))
    println("Customer at index 2: " + customers(2))

    println("Vector is useful because it provides fast indexed access and is immutable.")


    // 3. Map - product quantities and prices
    val productQuantities = Map(
      "Laptop" -> 2,
      "Phone" -> 5,
      "Tablet" -> 3
    )

    val productPrices = Map(
      "Laptop" -> 50000,
      "Phone" -> 20000,
      "Tablet" -> 15000
    )

    println("\nProduct Quantities:")
    println(productQuantities)

    println("Product Prices:")
    println(productPrices)


    // 4. For-comprehension combining customers and orders
    val customerOrders = List(
      ("Ravi", "Laptop"),
      ("Suresh", "Phone"),
      ("Priya", "Tablet"),
      ("Anjali", "Phone")
    )

    val combined = for {
      (customer, product) <- customerOrders
      price <- productPrices.get(product)
    } yield (customer, product, price)

    println("\nCustomer Orders:")
    combined.foreach(println)


    // 5. Daily sales summary without Spark
    val dailyOrders = List(
      ("Laptop", 2),
      ("Phone", 5),
      ("Tablet", 3),
      ("Phone", 2)
    )

    val dailySummary = dailyOrders
      .groupBy(_._1)
      .map {
        case (product, orders) =>
          val quantity = orders.map(_._2).reduce(_ + _)
          val price = productPrices(product)
          val revenue = quantity * price
          (product, quantity, revenue)
      }

    println("\nDaily Sales Summary:")

    dailySummary.foreach {
      case (product, quantity, revenue) =>
        println(
          s"Product: $product | Quantity: $quantity | Revenue: ₹$revenue"
        )
    }

    val grandTotal = dailySummary
      .map(_._3)
      .reduce(_ + _)

    println("\nGrand Total Sales: ₹" + grandTotal)
  }
}
