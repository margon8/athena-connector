package com.factorialhr

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler
import com.factorialhr.api.FactorialRecordHandler

case class FactorialCompositeHandler()
  extends CompositeHandler(new FactorialMetadataHandler() , new FactorialRecordHandler())
