package com.factorialhr

import com.amazonaws.athena.connector.lambda.QueryStatusChecker
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter
import com.amazonaws.athena.connector.lambda.data.{Block, BlockSpiller}
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest
import com.amazonaws.services.athena.AmazonAthenaClientBuilder
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder
import org.apache.logging.log4j.scala.Logging

import java.io.{BufferedReader, InputStreamReader}
import java.lang.String.format

class FactorialRecordHandler(amazonS3: AmazonS3 = AmazonS3ClientBuilder.defaultClient)
  extends RecordHandler(
    amazonS3,
    AWSSecretsManagerClientBuilder.defaultClient(),
    AmazonAthenaClientBuilder.defaultClient(),
    "factorial_api")
  with Logging {

  override def readWithConstraint(
         blockSpiller: BlockSpiller,
         readRecordsRequest: ReadRecordsRequest,
         queryStatusChecker: QueryStatusChecker): Unit = {

    logger.info("readWithConstraint: enter - " + readRecordsRequest.getSplit)

    val split = readRecordsRequest.getSplit
    val splitYear = 0
    val splitMonth = 0
    val splitDay = 0

    /**
     * TODO: Extract information about what we need to read from the split. If you are following the tutorial
     * this is basically the partition column values for year, month, day.
     *
     * splitYear = split.getPropertyAsInt("year");
     * splitMonth = split.getPropertyAsInt("month");
     * splitDay = split.getPropertyAsInt("day");
     *
     */

    val dataBucket = null
    /**
     * TODO: Get the data bucket from the env variable set by athena-example.yaml
     *
     * dataBucket = System.getenv("data_bucket");
     *
     */

    val dataKey = format("%s/%s/%s/sample_data.csv", splitYear, splitMonth, splitDay)

    val s3Reader = openS3File(amazonS3, dataBucket, dataKey)
    if (s3Reader == null) { //There is no data to read for this split.
      return
    }

    val builder = GeneratedRowWriter.newBuilder(readRecordsRequest.getConstraints)

    /**
     * TODO: Add extractors for each field to our RowWRiterBuilder, the RowWriterBuilder will then 'generate'
     * optomized code for converting our data to Apache Arrow, automatically minimizing memory overhead, code
     * branches, etc... Later in the code when we call RowWriter for each line in our S3 file
     *
     * builder.withExtractor("year", (IntExtractor) (Object context, NullableIntHolder value) -> {
     * value.isSet = 1;
     * value.value = Integer.parseInt(((String[]) context)[0]);
     * });
     *
     * builder.withExtractor("month", (IntExtractor) (Object context, NullableIntHolder value) -> {
     * value.isSet = 1;
     * value.value = Integer.parseInt(((String[]) context)[1]);
     * });
     *
     * builder.withExtractor("day", (IntExtractor) (Object context, NullableIntHolder value) -> {
     * value.isSet = 1;
     * value.value = Integer.parseInt(((String[]) context)[2]);
     * });
     *
     * builder.withExtractor("encrypted_payload", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
     * value.isSet = 1;
     * value.value = ((String[]) context)[6];
     * });
     */

    /**
     * TODO: The account_id field is a sensitive field, so we'd like to mask it to the last 4 before
     * returning it to Athena. Note that this will mean you can only filter (where/having)
     * on the masked value from Athena.
     *
     * builder.withExtractor("account_id", (VarCharExtractor) (Object context, NullableVarCharHolder value) -> {
     * value.isSet = 1;
     * String accountId = ((String[]) context)[3];
     * value.value = accountId.length() > 4 ? accountId.substring(accountId.length() - 4) : accountId;
     * });
     */

    /**
     * TODO: Write data for our transaction STRUCT:
     * For complex types like List and Struct, we can build a Map to conveniently set nested values
     *
     * builder.withFieldWriterFactory("transaction",
     * (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
     * (Object context, int rowNum) -> {
     * Map<String, Object> eventMap = new HashMap<>();
     * eventMap.put("id", Integer.parseInt(((String[])context)[4]));
     * eventMap.put("completed", Boolean.parseBoolean(((String[])context)[5]));
     * BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, eventMap);
     * return true;    //we don't yet support predicate pushdown on complex types
     * });
     */

    //Used some basic code-gen to optimize how we generate response data.
    val rowWriter = builder.build

    //We read the transaction data line by line from our S3 object.
    s3Reader
      .lines()
      .map { line =>
        logger.info("readWithConstraint: processing line " + line)
        //The sample_data.csv file is structured as year,month,day,account_id,transaction.id,transaction.complete
        val lineParts = line.split(",")
        //We use the provided BlockSpiller to write our row data into the response. This utility is provided by
        //the Amazon Athena Query Federation SDK and automatically handles breaking the data into reasonably sized
        //chunks, encrypting it, and spilling to S3 if we've enabled these features.
        blockSpiller.writeRows((block: Block, rowNum: Int) => if (rowWriter.writeRow(block, rowNum, lineParts)) 1
        else 0)
      }
  }

  private def openS3File(amazonS3: AmazonS3, bucket: String, key: String): BufferedReader = {
    logger.info("openS3File: opening file " + bucket + ":" + key)
    if (amazonS3.doesObjectExist(bucket, key)) {
      val obj = amazonS3.getObject(bucket, key)
      logger.info("openS3File: opened file " + bucket + ":" + key)
      return new BufferedReader(new InputStreamReader(obj.getObjectContent))
    }
    null
  }
}
