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
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CiderVariableReferenceExpression
        extends CiderExpression
{
    private int columnIndex;
    private String targetType;
    private String name;

    public CiderVariableReferenceExpression(int columnIndex, String targetType)
    {
        this.columnIndex = columnIndex;
        this.targetType = targetType;
    }

    public CiderVariableReferenceExpression(String name, String targetType, int columnIndex)
    {
        this.name = name;
        this.targetType = targetType;
        this.columnIndex = columnIndex;
    }

    @Override
    public ObjectNode toJson(ObjectMapper objectManager)
    {
        return null;
    }

    public void toJson(ObjectNode operandsInput, ObjectNode operandsFeilds)
    {
        // column index
        operandsInput.put("input", columnIndex);
        operandsFeilds.put("target_type", matchType(targetType).toUpperCase());
        operandsFeilds.put("type_scale", getTypeScale(targetType));
        operandsFeilds.put("type_precision", getTypePrecision(targetType));
    }

    protected String matchType(String type)
    {
        switch (type) {
            case "int":
            case "integer":
            case "Int":
                return "Integer";
            case "double":
                return "decimal";
        }
        return type;
    }

    protected int getTypeScale(String type)
    {
        switch (type) {
            case "int":
            case "integer":
            case "Int":
                return 0;
            case "double":
                return 2;
        }
        return -1;
    }

    protected int getTypePrecision(String type)
    {
        switch (type) {
            case "int":
            case "integer":
            case "Int":
                return 10;
            case "double":
                return 5;
        }
        return -1;
    }
}
