package io.github.toddlipcon;

import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduClient.KuduClientBuilder;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class KuduOutput implements DbOutput {

  private final String masterAddrs;
  private KuduClient client;

  public KuduOutput(String masterAddrs) {
    Preconditions.checkArgument(masterAddrs != null, "must specify masters");
    this.masterAddrs = masterAddrs;
  }

  @Override
  public void init() {
    client = new KuduClientBuilder(masterAddrs)
        .defaultOperationTimeoutMs(60000)
        .bossCount(4)
        .workerCount(8)
        .build();
  }

  @Override
  public void createTablesIfNecessary() {
    CreateTableOptions opts = new CreateTableOptions()
      .addHashPartitions(ImmutableList.of("l_orderkey"), 50)
      .setNumReplicas(3);
    Schema schema = new Schema(ImmutableList.<ColumnSchema>builder()
        .add(new ColumnSchemaBuilder("l_orderkey", Type.INT64).key(true).nullable(false).build())
        .add(new ColumnSchemaBuilder("l_linenumber", Type.INT64).key(true).nullable(false).build())
        .add(new ColumnSchemaBuilder("l_partkey", Type.INT64).nullable(false).build())
        .add(new ColumnSchemaBuilder("l_suppkey", Type.INT64).nullable(false).build())
        .add(new ColumnSchemaBuilder("l_quantity", Type.DOUBLE).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_extendedprice", Type.DOUBLE).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_discount", Type.DOUBLE).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_tax", Type.DOUBLE).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_returnflag", Type.STRING).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_linestatus", Type.STRING).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_shipdate", Type.STRING).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_commitdate", Type.STRING).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_receiptdate", Type.STRING).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_shipinstruct", Type.STRING).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_shipmode", Type.STRING).nullable(true).build())
        .add(new ColumnSchemaBuilder("l_comment", Type.STRING).nullable(true).build())
        .build());
    try {
      client.createTable("lineitem", schema, opts);
    } catch (KuduException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeChunk(List<LineItem> chunk) {
    KuduTable t;
    try {
      t = client.openTable("lineitem");
    } catch (KuduException e) {
      throw new RuntimeException(e);
    }
    KuduSession s = client.newSession();
    try {
      s.setFlushMode(FlushMode.MANUAL_FLUSH);
      for (LineItem l : chunk) {
        Insert ins = t.newInsert();
        ins.getRow().addLong("l_orderkey", l.orderKey);
        ins.getRow().addLong("l_partkey", l.partKey);
        ins.getRow().addLong("l_suppkey", l.suppKey);
        ins.getRow().addLong("l_linenumber", l.lineNumber);
        ins.getRow().addDouble("l_quantity", l.quantity);
        ins.getRow().addDouble("l_extendedprice", l.extendedPrice);
        ins.getRow().addDouble("l_discount", l.discount);
        ins.getRow().addDouble("l_tax", l.tax);
        ins.getRow().addString("l_returnflag", l.returnFlag);
        ins.getRow().addString("l_linestatus", l.lineStatus);
        ins.getRow().addString("l_shipdate", l.shipDate);
        ins.getRow().addString("l_commitdate", l.commitDate);
        ins.getRow().addString("l_receiptdate", l.receiptDate);
        ins.getRow().addString("l_shipinstruct", l.shipInstruct);
        ins.getRow().addString("l_shipmode", l.shipMode);
        ins.getRow().addString("l_comment", l.comment);
        s.apply(ins);
      }
      List<OperationResponse> resp = s.flush();
      for (OperationResponse r : resp) {
        if (r.hasRowError()) {
          throw new RuntimeException(r.getRowError().toString());
        }
      }
    } catch (KuduException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        s.close();
      } catch (KuduException e) {
        // ignore close error
      }
    }
  }


}
