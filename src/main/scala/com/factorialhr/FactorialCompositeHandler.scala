package com.factorialhr

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler

case class FactorialCompositeHandler()
  extends CompositeHandler(new FactorialMetadataHandler() , new FactorialRecordHandler())
