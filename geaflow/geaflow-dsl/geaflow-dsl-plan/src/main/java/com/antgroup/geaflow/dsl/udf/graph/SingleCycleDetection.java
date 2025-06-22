/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.common.type.primitive.StringType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

//全局静态字段
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Description(name = "single_cycle_detection", description = "Improved built-in udga for single node cycle detection with String vertex IDs.")
public class SingleCycleDetection implements AlgorithmUserFunction<Object, ObjectRow> {

    private AlgorithmRuntimeContext<Object, ObjectRow> context;

    private  int maxIteration = 10;
    private  int minIteration = 3;
    private  int limitNumber = 100;

    private String targetVertexId = null;
    private static final AtomicLong GLOBAL_CYCLE_COUNTER = new AtomicLong(0);

    @Override
    public void init(AlgorithmRuntimeContext<Object, ObjectRow> context, Object[] params) {
        this.context = context;
        if (params.length == 1) {
            assert params[0] instanceof String : "Target vertex ID parameter should be String.";
            targetVertexId = (String) params[0];
        } else if (params.length == 2) {
            assert params[0] instanceof String : "Target vertex ID parameter should be String.";
            targetVertexId = (String) params[0];
            assert params[1] instanceof Integer : "MinIteration parameter should be Integer.";
            minIteration = (Integer) params[1];
        } else if (params.length == 3) {
            assert params[0] instanceof String : "Target vertex ID parameter should be String.";
            targetVertexId = (String) params[0];
            assert params[1] instanceof Integer : "MinIteration parameter should be Integer.";
            minIteration = (Integer) params[1];
            assert params[2] instanceof Integer : "MaxIteration parameter should be Integer.";
            maxIteration = (Integer) params[2];
        } else if (params.length == 4) {
            assert params[0] instanceof String : "Target vertex ID parameter should be String.";
            targetVertexId = (String) params[0];
            assert params[1] instanceof Integer : "MinIteration parameter should be Integer.";
            minIteration = (Integer) params[1];
            assert params[2] instanceof Integer : "MaxIteration parameter should be Integer.";
            maxIteration = (Integer) params[2];
            assert params[3] instanceof Integer : "LimitNumber parameter should be Integer.";
            limitNumber = (Integer) params[3];
        } else {
            throw new IllegalArgumentException("SingleCycleDetection requires at least one parameter: targetVertexId.");
        }
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<ObjectRow> messages) {
        long currentIteration = context.getCurrentIterationId();

        if (GLOBAL_CYCLE_COUNTER.get() >= limitNumber) {
            return;
        }

        updatedValues.ifPresent(vertex::setValue);
        String currentVertexId = vertex.getId().toString();

        if (currentIteration == 1L) {
            if (Objects.equals(currentVertexId, targetVertexId)) {
                List<Object> initialPath = Lists.newArrayList();
                initialPath.add(1L); // path length
                initialPath.add(targetVertexId); // starting point

                ObjectRow initialMsg = ObjectRow.create(initialPath.toArray());
                for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                    context.sendMessage(edge.getTargetId(), initialMsg);
                }
            }
        } else if (currentIteration <= maxIteration + 1) {
            while (messages.hasNext()) {
                ObjectRow msg = messages.next();
                List<String> currentPath = rowToPath(msg);
                long currentPathLength = (long) msg.getField(0, LongType.INSTANCE);

                if (currentPath.contains(currentVertexId) && ! Objects.equals(currentVertexId, targetVertexId)) {
                    continue;
                }

                if (Objects.equals(currentVertexId, targetVertexId)
                        && currentIteration - 1 >= minIteration
                        && GLOBAL_CYCLE_COUNTER.get() < limitNumber) {

                    List<String> completeCycle = Lists.newArrayList(currentPath);
                    completeCycle.add(currentVertexId);

                    if (completeCycle.subList(1, completeCycle.size() - 1).contains(targetVertexId)) {
                        continue;
                    }


                    GLOBAL_CYCLE_COUNTER.incrementAndGet();
                    context.take(ObjectRow.create(
                        targetVertexId,
                        pathToString(completeCycle),
                        completeCycle.size() - 1
                    ));
                    continue;
                }

                List<Object> newPath = Lists.newArrayList();
                newPath.add(currentPathLength + 1);
                newPath.addAll(currentPath);
                newPath.add(currentVertexId);

                ObjectRow newMsg = ObjectRow.create(newPath.toArray());
                for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                    context.sendMessage(edge.getTargetId(), newMsg);
                }
            }
        }
    }

    @Override
    public void finish(RowVertex graphVertex, Optional<Row> updatedValues) {
        // No-op
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("target_vertex_id", StringType.INSTANCE, false),
            new TableField("cycle_path", StringType.INSTANCE, false),
            new TableField("cycle_length", LongType.INSTANCE, false)
        );
    }

    private List<String> rowToPath(ObjectRow row) {
        List<String> path = Lists.newArrayList();
        long pathLength = (long) row.getField(0, LongType.INSTANCE);
        for (int i = 0; i < pathLength; i++) {
            path.add((String) row.getField(i + 1, StringType.INSTANCE));
        }
        return path;
    }

    private String pathToString(List<String> path) {
        return path.stream().map(String::valueOf).collect(Collectors.joining(" → "));
    }
}
