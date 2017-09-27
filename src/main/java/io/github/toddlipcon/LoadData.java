package io.github.toddlipcon;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.api.client.repackaged.com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LoadData {
  private static final int CHUNK_SIZE = Integer.getInteger("chunkSize", 1000);
  private static final int INSERT_THREADS = 8;
  private static final int CSV_THREADS = 8;

  private static final Logger LOG = LoggerFactory.getLogger(LoadData.class);

  private final ExecutorService csvExec = Executors.newFixedThreadPool(CSV_THREADS,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("csv-%s").build());

  private final ExecutorService insertExec = Executors.newFixedThreadPool(INSERT_THREADS,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("insert-%s").build());
  private final Semaphore insertSem = new Semaphore(INSERT_THREADS);
  private DbOutput output;

  public LoadData(Map<String, String> config) {
    String mode = config.get("mode");
    if ("spanner".equals(mode)) {
      this.output = new SpannerOutput(config.get("instance-id"), config.get("db"));
    } else if ("kudu".equals(mode)) {
      this.output = new KuduOutput(config.get("masters"));
    } else {
      throw new IllegalArgumentException("invalid mode: " + mode);
    }
  }

  static void printUsageAndExit() {
    System.err.println("Usage:");
    System.err.println("    LoadData <writer-config> <paths>");
    System.err.println("writer-config should be either:");
    System.err.println(" mode=spanner&instance=<instance-id>&db=<db-id>");
    System.err.println("or");
    System.err.println(" mode=kudu&masters=<comma-separated masters>");
    System.exit(1);
  }

  public static void main(String[] args) throws JsonProcessingException, IOException, InterruptedException {
    if (args.length < 2) {
      printUsageAndExit();
    }
    Map<String, String> conf = Splitter.on("&").withKeyValueSeparator("=").split(args[0]);
    new LoadData(conf).run(Arrays.copyOfRange(args, 1, args.length));
  }

  private void run(String[] args) throws InterruptedException {
    output.init();
    output.createTablesIfNecessary();
    for (String path : args) {
      csvExec.submit(() -> {
        parseAndLoad(path);
        return null;
      });
    }
    csvExec.awaitTermination(10, TimeUnit.DAYS);
  }

  private void parseAndLoad(String path) throws JsonProcessingException, IOException, InterruptedException {
    CsvMapper mapper = new CsvMapper();
    CsvSchema schema = mapper.schemaFor(LineItem.class)
        .withColumnSeparator('|');
    MappingIterator<LineItem> it = mapper
        .readerFor(LineItem.class)
        .with(schema)
        .readValues(new File(path));
    List<LineItem> chunk = Lists.newArrayListWithCapacity(CHUNK_SIZE);
    while (it.hasNextValue()) {
      LineItem li = it.nextValue();
      chunk.add(li);
      if (chunk.size() == CHUNK_SIZE) {
        final List<LineItem> finalChunk = chunk;
        chunk = Lists.newArrayListWithCapacity(CHUNK_SIZE);
        insertSem.acquire();
        insertExec.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            try {
              output.writeChunk(finalChunk);
              LOG.info("wrote chunk of {} rows", finalChunk.size());
            } catch (Exception e) {
              LOG.warn("Failed to write a chunk", e);
              throw e;
            } finally {
              insertSem.release();
            }
            return null;
          }
        });
      }
    }

  }
}
