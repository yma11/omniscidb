/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mapd.plan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javafx.util.Pair;

import java.util.HashMap;
import java.util.List;

public class CiderProjectNode
        extends CiderOperatorNode
{
    // TODO: remove fieldNames and exprs
    private List<String> fieldNames;
    private List<Pair<String, Integer>> exprs;
    private HashMap<CiderVariableReferenceExpression, CiderExpression> assignments;
    //column, based on TableScannode outputVariables(a, b, c)

    // TODO: remove this constructor
    public CiderProjectNode(List<String> fieldNames, List<Pair<String, Integer>> exprs)
    {
        super("LogicalProject");
        this.fieldNames = fieldNames;
        this.exprs = exprs;
    }

    public CiderProjectNode(HashMap<CiderVariableReferenceExpression, CiderExpression> assignments)
    {
        super("LogicalProject");
        this.assignments = assignments;
    }

    @Override
    protected ObjectNode toJson(ObjectMapper objectMapper, String id)
    {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("id", id);
        objectNode.put("relOp", opName);
        ArrayNode exprsFeilds = objectMapper.createArrayNode();
        ArrayNode feildNodes = objectMapper.createArrayNode();
        int i = 0;
        for (Pair<String, Integer> expr : exprs) {
            ObjectNode exprsFeild = objectMapper.createObjectNode();
            feildNodes.insert(i++, expr.getKey());
            exprsFeild.put("input", expr.getValue());
            exprsFeilds.add(exprsFeild);
        }
        objectNode.set("fields", feildNodes);
        objectNode.set("exprs", exprsFeilds);
        return objectNode;
    }
}
