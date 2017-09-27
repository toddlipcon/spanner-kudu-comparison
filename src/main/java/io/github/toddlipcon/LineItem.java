package io.github.toddlipcon;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"orderKey", "partKey", "suppKey", "lineNumber",
  "quantity", "extendedPrice", "discount", "tax", "returnFlag", "lineStatus",
  "shipDate", "commitDate", "receiptDate", "shipInstruct", "shipMode", "comment"})
public class LineItem {
  public long orderKey;
  public long partKey;
  public long suppKey;
  public int lineNumber;
  public float quantity;
  public float extendedPrice;
  public float discount;
  public float tax;
  public String returnFlag;
  public String lineStatus;
  public String shipDate;
  public String commitDate;
  public String receiptDate;
  public String shipInstruct;
  public String shipMode;
  public String comment;
}