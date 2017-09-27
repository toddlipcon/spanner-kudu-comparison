package io.github.toddlipcon;

import java.util.List;

public interface DbOutput {

  void writeChunk(List<LineItem> chunk);

  void createTablesIfNecessary();

  void init();

}