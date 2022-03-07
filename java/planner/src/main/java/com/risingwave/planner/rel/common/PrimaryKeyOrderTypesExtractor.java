package com.risingwave.planner.rel.common;

import static java.util.Objects.requireNonNull;

import com.risingwave.proto.plan.OrderType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.lang3.SerializationException;

/** A utility class for getting order types from RelCollation */
public class PrimaryKeyOrderTypesExtractor {

  /**
   * @param relCollation RelCollation of a input
   * @param primaryKeyColumnIndices Primary key column indices
   * @return Order types of column indexed by primaryKeyColumnIndices
   */
  public static List<OrderType> getPrimaryKeyColumnOrderTypes(
      RelCollation relCollation, List<Integer> primaryKeyColumnIndices) {
    requireNonNull(primaryKeyColumnIndices, "primaryKeyColumnIndices is null");
    if (relCollation == null) {
      return primaryKeyColumnIndices.stream()
          .map(idx -> OrderType.ASCENDING)
          .collect(Collectors.toList());
    }
    var orderTypes = new ArrayList<OrderType>();
    List<RelFieldCollation> relFieldCollations = relCollation.getFieldCollations();
    HashMap<Integer, RelFieldCollation> inputIndexToCollation = new HashMap<>();
    for (var relFieldCollation : relFieldCollations) {
      inputIndexToCollation.put(relFieldCollation.getFieldIndex(), relFieldCollation);
    }
    for (var primaryKeyIndex : primaryKeyColumnIndices) {
      if (inputIndexToCollation.containsKey(primaryKeyIndex)) {
        var relFieldCollation = inputIndexToCollation.get(primaryKeyIndex);
        RelFieldCollation.Direction dir = relFieldCollation.getDirection();
        OrderType orderType;
        if (dir == RelFieldCollation.Direction.ASCENDING) {
          orderType = OrderType.ASCENDING;
        } else if (dir == RelFieldCollation.Direction.DESCENDING) {
          orderType = OrderType.DESCENDING;
        } else {
          throw new SerializationException(String.format("%s direction not supported", dir));
        }
        orderTypes.add(orderType);
      } else {
        orderTypes.add(OrderType.ASCENDING);
      }
    }
    return orderTypes;
  }
}
