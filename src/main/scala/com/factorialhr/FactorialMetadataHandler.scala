package com.factorialhr

import com.amazonaws.athena.connector.lambda.QueryStatusChecker
import com.amazonaws.athena.connector.lambda.data.{Block, BlockAllocator, BlockWriter, SchemaBuilder}
import com.amazonaws.athena.connector.lambda.domain.{Split, TableName}
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler
import com.amazonaws.athena.connector.lambda.metadata._
import org.apache.arrow.vector.complex.reader.FieldReader
import org.apache.logging.log4j.scala.Logging

import scala.jdk.CollectionConverters._

class FactorialMetadataHandler
    extends MetadataHandler("factorial_api")
    with Logging {

  override def doListSchemaNames(blockAllocator: BlockAllocator, listSchemasRequest: ListSchemasRequest): ListSchemasResponse = {

    logger.info("doListSchemaNames: enter - " + listSchemasRequest)

    val schemas = Set("reports")

    new ListSchemasResponse(listSchemasRequest.getCatalogName, schemas.asJava)
  }

  override def doListTables(blockAllocator: BlockAllocator, listTablesRequest: ListTablesRequest): ListTablesResponse = {
    logger.info("doListTables: enter - " + listTablesRequest)

    val tables =
      Seq("companies", "customers")
      .map {
        new TableName(listTablesRequest.getSchemaName, _)
      }

    return new ListTablesResponse(listTablesRequest.getCatalogName, tables.asJava)

  }

  override def doGetTable(blockAllocator: BlockAllocator, getTableRequest: GetTableRequest): GetTableResponse = {

    logger.info("doGetTable: enter - " + getTableRequest)

    val partitionColNames: Set[String] = Set("")

    /**
     * TODO: Add partitions columns, example below.
     *
     * partitionColNames.add("year");
     * partitionColNames.add("month");
     * partitionColNames.add("day");
     *
     */

    val tableSchemaBuilder = SchemaBuilder.newBuilder

    /**
     * TODO: Generate a schema for the requested table.
     *
     * tableSchemaBuilder.addIntField("year")
     * .addIntField("month")
     * .addIntField("day")
     * .addStringField("account_id")
     * .addStringField("encrypted_payload")
     * .addStructField("transaction")
     * .addChildField("transaction", "id", Types.MinorType.INT.getType())
     * .addChildField("transaction", "completed", Types.MinorType.BIT.getType())
     * //Metadata who's name matches a column name
     * //is interpreted as the description of that
     * //column when you run "show tables" queries.
     * .addMetadata("year", "The year that the payment took place in.")
     * .addMetadata("month", "The month that the payment took place in.")
     * .addMetadata("day", "The day that the payment took place in.")
     * .addMetadata("account_id", "The account_id used for this payment.")
     * .addMetadata("encrypted_payload", "A special encrypted payload.")
     * .addMetadata("transaction", "The payment transaction details.")
     * //This metadata field is for our own use, Athena will ignore and pass along fields it doesn't expect.
     * //we will use this later when we implement doGetTableLayout(...)
     * .addMetadata("partitionCols", "year,month,day");
     *
     */

    new GetTableResponse(
      getTableRequest.getCatalogName,
      getTableRequest.getTableName,
      tableSchemaBuilder.build,
      partitionColNames.asJava)

  }

  override def getPartitions(blockWriter: BlockWriter, getTableLayoutRequest: GetTableLayoutRequest,
                             queryStatusChecker: QueryStatusChecker): Unit = {

    for (year <- 2000 until 2018) {
      for (month <- 1 until 12) {
        for (day <- 1 until 31) {
          val yearVal = year
          val monthVal = month
          val dayVal = day

          /**
           * TODO: If the partition represented by this year,month,day offer the values to the block
           * and check if they all passed constraints. The Block has been configured to automatically
           * apply our partition pruning constraints.
           *
           * blockWriter.writeRows((Block block, int row) -> {
           * boolean matched = true;
           * matched &= block.setValue("year", row, yearVal);
           * matched &= block.setValue("month", row, monthVal);
           * matched &= block.setValue("day", row, dayVal);
           * //If all fields matches then we wrote 1 row during this call so we return 1
           * return matched ? 1 : 0;
           * });
           *
           */
        }
      }
    }

  }

  override def doGetSplits(blockAllocator: BlockAllocator, getSplitsRequest: GetSplitsRequest): GetSplitsResponse = {

    logger.info("doGetSplits: enter - " + getSplitsRequest)

    val catalogName: String = getSplitsRequest.getCatalogName
    val splits: Set[Split] = Set()

    val partitions: Block = getSplitsRequest.getPartitions

    val day: FieldReader = partitions.getFieldReader("day")
    val month: FieldReader = partitions.getFieldReader("month")
    val year: FieldReader = partitions.getFieldReader("year")
    for (i <- 0 until partitions.getRowCount) { //Set the readers to the partition row we area on
      year.setPosition(i)
      month.setPosition(i)
      day.setPosition(i)

      /**
       * TODO: For each partition in the request, create 1 or more splits. Splits
       * are parallelizable units of work. Each represents a part of your table
       * that needs to be read for the query. Splits are opaque to Athena aside from the
       * spill location and encryption key. All properties added to a split are solely
       * for your use when Athena calls your readWithContraints(...) function to perform
       * the read. In this example we just need to know the partition details (year, month, day).
       *
       * Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
       * .add("year", String.valueOf(year.readInteger()))
       * .add("month", String.valueOf(month.readInteger()))
       * .add("day", String.valueOf(day.readInteger()))
       * .build();
       *
       * splits.add(split);
       *
       */
    }

    logger.info("doGetSplits: exit - " + splits.size)
    return new GetSplitsResponse(catalogName, splits.asJava)
  }
}
