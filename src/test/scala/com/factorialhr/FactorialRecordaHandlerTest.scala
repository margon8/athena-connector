package com.factorialhr

import org.scalatest.funsuite.AnyFunSuite

class FactorialRecordaHandlerTest extends AnyFunSuite {

  test("Get bearer") {
    val f = new FactorialRecordHandler()
    val bearer = f.authAPIConnection
    println(bearer)
  }

}
