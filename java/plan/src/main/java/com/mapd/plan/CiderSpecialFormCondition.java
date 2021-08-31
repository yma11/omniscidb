package com.mapd.plan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class CiderSpecialFormCondition extends CiderExpression {

    private Form form;
    private String type;
    private List<CiderExpression> conditions;

    public CiderSpecialFormCondition(Form form, String type, CiderExpression... conditions) {
        this(form, type,  unmodifiableList(Arrays.asList(conditions)));
    }

    public CiderSpecialFormCondition(Form form, String type, List<CiderExpression> conditions) {
        this.form = form;
        this.type = type;
        this.conditions = conditions;
    }

    @Override
    public ObjectNode toJson(ObjectMapper objectMapper)
    {
        return null;
    }
    public enum Form
    {
        IF,
        NULL_IF,
        SWITCH,
        WHEN,
        IS_NULL,
        COALESCE,
        IN,
        AND,
        OR,
        DEREFERENCE,
        ROW_CONSTRUCTOR,
        BIND,
    }
}
