package com.factorialhr

import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.Context

import com.google.gson.Gson
import com.google.gson.GsonBuilder

// Handler value: example.Handler
class Handler extends RequestHandler[String, String] {

  val gson: Gson = new GsonBuilder().setPrettyPrinting.create

  def handleRequest(event: String, context: Context): String = {

    val logger = context.getLogger
    val response = "200 OK"
    // log execution details
    logger.log("ENVIRONMENT VARIABLES: " + gson.toJson(System.getenv))
    logger.log("CONTEXT: " + gson.toJson(context))
    // process event
    logger.log("EVENT: " + gson.toJson(event))
    logger.log("EVENT TYPE: " + event.getClass)
    response
  }
}
