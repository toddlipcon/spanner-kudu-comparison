package io.github.toddlipcon;

import java.util.Arrays;
import java.util.List;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.Iterables;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;

public class SpannerOutput implements DbOutput {
  private final String instanceId;
  private final String databaseId;

  private DatabaseClient client;
  private DatabaseAdminClient adminClient;


  public SpannerOutput(String instanceId, String databaseId) {
    this.instanceId = instanceId;
    this.databaseId = databaseId;
  }

  private DatabaseId getDatabaseId() {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    return DatabaseId.of(options.getProjectId(), instanceId, databaseId);
  }

  @Override
  public void init() {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    this.client = spanner.getDatabaseClient(getDatabaseId());
    this.adminClient = spanner.getDatabaseAdminClient();
  }

  @Override
  public void createTablesIfNecessary() {
    DatabaseId id = getDatabaseId();
    Operation<Database, CreateDatabaseMetadata> op = adminClient
        .createDatabase(
            id.getInstanceId().getInstance(),
            id.getDatabase(),
            Arrays.asList(
                "CREATE TABLE LINEITEM (\n" +
                "  L_ORDERKEY    INT64 NOT NULL,\n" +
                "  L_PARTKEY   INT64 NOT NULL,\n" +
                "  L_SUPPKEY   INT64 NOT NULL,\n" +
                "  L_LINENUMBER  INT64,\n" +
                "  L_QUANTITY    FLOAT64,\n" +
                "  L_EXTENDEDPRICE FLOAT64,\n" +
                "  L_DISCOUNT    FLOAT64,\n" +
                "  L_TAX     FLOAT64,\n" +
                "  L_RETURNFLAG  STRING(1),\n" +
                "  L_LINESTATUS  STRING(1),\n" +
                "  L_SHIPDATE    DATE,\n" +
                "  L_COMMITDATE  DATE,\n" +
                "  L_RECEIPTDATE DATE,\n" +
                "  L_SHIPINSTRUCT  STRING(25),\n" +
                "  L_SHIPMODE    STRING(10),\n" +
                "  L_COMMENT   STRING(44)\n" +
                ")  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)\n"));
    Database db = op.waitFor().getResult();
    System.out.println("Created database [" + db.getId() + "]");
  }


  @Override
  public void writeChunk(List<LineItem> chunk) {
    Iterable<Mutation> mutations = Iterables.transform(chunk, l -> {
      return Mutation.newInsertBuilder("LINEITEM")
      .set("L_ORDERKEY").to(l.orderKey)
      .set("L_PARTKEY").to(l.partKey)
      .set("L_SUPPKEY").to(l.suppKey)
      .set("L_LINENUMBER").to(l.lineNumber)
      .set("L_QUANTITY").to(l.quantity)
      .set("L_EXTENDEDPRICE").to(l.extendedPrice)
      .set("L_DISCOUNT").to(l.discount)
      .set("L_TAX").to(l.tax)
      .set("L_RETURNFLAG").to(l.returnFlag)
      .set("L_LINESTATUS").to(l.lineStatus)
      .set("L_SHIPDATE").to(l.shipDate)
      .set("L_COMMITDATE").to(l.commitDate)
      .set("L_RECEIPTDATE").to(l.receiptDate)
      .set("L_SHIPINSTRUCT").to(l.shipInstruct)
      .set("L_SHIPMODE").to(l.shipMode)
      .set("L_COMMENT").to(l.comment)
      .build();
    });
    Timestamp write = client.write(mutations);
    System.out.println("wrote " + chunk.size() + " rows at " + write);
  }
}
