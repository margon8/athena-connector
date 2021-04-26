package com.factorialhr

import com.factorialhr.api.FactorialRecordHandler
import org.scalatest.funsuite.AnyFunSuite

class FactorialRecordaHandlerTest extends AnyFunSuite {

  test("Get bearer") {
    val f = new FactorialRecordHandler()
    val bearer = f.authAPIConnection
    println(bearer)
  }

}
