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
package com.facebook.presto.druid;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.druid.DruidQueryGeneratorContext.Origin;
import com.facebook.presto.druid.DruidQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.druid.DruidExpression.derived;
import static com.facebook.presto.druid.DruidPushdownUtils.getLiteralAsString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
/**
 * This expression converter will convert presto expressions to dql statements that will be sent to the
 * sys tables. NOTE: The schema for the segement's start/end time is a string of ISO-861. So each potential value
 * will be a combo of start >= and end <, using the natural order of string characters.
 */
public class DruidSegmentFilterExpressionConverter
        implements RowExpressionVisitor<DruidExpression, Function<VariableReferenceExpression, Selection>>
{
    private static final Set<String> LOGICAL_BINARY_OPS_FILTER = ImmutableSet.of("=", "<", "<=", ">", ">=", "<>");
    private static final String __TIME = "\"__time\"";
    private static final String START_SEG = "\"start\"";
    private static final String END_SEG = "\"end\"";

    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ConnectorSession session;
    private DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendInstant(3).parseCaseInsensitive().toFormatter();

    public DruidSegmentFilterExpressionConverter(
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            ConnectorSession session)
    {
        this.typeManager = requireNonNull(typeManager, "type manager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.session = requireNonNull(session, "session is null");
    }

    private String convertForIso8601(ConstantExpression cnstExpr)
    {
        Type type = cnstExpr.getType();
        if (type instanceof AbstractLongType) {
            if (type instanceof TimestampWithTimeZoneType) {
                SqlTimestampWithTimeZone sqlTs = new SqlTimestampWithTimeZone((long) cnstExpr.getValue());
                return formatter.format(Instant.ofEpochMilli(sqlTs.getMillisUtc()));
            }
            else {
                return formatter.format(Instant.ofEpochMilli((long) cnstExpr.getValue()));
            }
        }
        return null;
    }

    private DruidExpression handleIn(
            SpecialFormExpression specialForm,
            boolean isWhitelist,
            Function<VariableReferenceExpression, Selection> context)
    {
        String name = specialForm.getArguments().get(0).accept(this, context).getDefinition();
        if (!name.equals(__TIME)) {
            return new DruidExpression("", Origin.DERIVED);
        }
        else {
            List<String> predicates = new ArrayList<>();
            for (RowExpression re : specialForm.getArguments().subList(1, specialForm.getArguments().size())) {
                if (re instanceof ConstantExpression) {
                    ConstantExpression ce = (ConstantExpression) re;
                    String instant = convertForIso8601(ce);
                    if (instant != null) {
                        predicates.add(format(
                                "(%s %s '%s' AND %s %s '%s')",
                                START_SEG,
                                ">=",
                                instant,
                                END_SEG,
                                "<=",
                                instant));
                    }
                    else {
                        return new DruidExpression("", Origin.DERIVED);
                    }
                }
                else {
                    return new DruidExpression("", Origin.DERIVED);
                }
            }
            if (predicates.isEmpty()) {
                return new DruidExpression("", Origin.DERIVED);
            }

            // At this point build the longer where clause.
            StringBuilder sb = new StringBuilder();
            for (String predicate : predicates) {
                sb.append(predicate);
                sb.append(" OR ");
            }
            return new DruidExpression(sb.substring(0, sb.length() - 4), Origin.DERIVED);
        }
    }

    private DruidExpression handleLogicalBinary(
            String operator,
            CallExpression call,
            Function<VariableReferenceExpression, Selection> context)
    {
        if (!LOGICAL_BINARY_OPS_FILTER.contains(operator)) {
            throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, operator + " is not supported in Druid filter");
        }
        if (operator.equals("<>")) {
            return new DruidExpression("", Origin.DERIVED);
        }
        List<RowExpression> arguments = call.getArguments();
        if (arguments.size() == 2) {
            String name = arguments.get(0).accept(this, context).getDefinition();
            if (!name.equals(__TIME)) {
                return new DruidExpression("", Origin.DERIVED);
            }
            else {
                // Ensure the value is a supported type.
                RowExpression re = arguments.get(1);
                if (re instanceof ConstantExpression) {
                    ConstantExpression ce = (ConstantExpression) re;
                    String instant = convertForIso8601(ce);
                    if (instant != null) {
                        if (operator.equals(">") || operator.equals(">=") || operator.equals("<") || operator.equals("<=")) {
                            String startEnd = operator.equals(">") || operator.equals(">=") ? START_SEG : END_SEG;
                            if (operator.equals(">")) {
                                operator = ">=";
                            }
                            if (operator.equals("<")) {
                                operator = "<=";
                            }
                            return derived(format(
                                    "(%s %s '%s')",
                                    startEnd,
                                    operator,
                                    instant));
                        }
                        else {
                            return derived(format(
                                    "(%s %s '%s' AND %s %s '%s')",
                                    START_SEG,
                                    ">=",
                                    instant,
                                    END_SEG,
                                    "<=",
                                    instant));
                        }
                    }
                }
                else {
                    // Only constant expression are suppored for this optimization
                    return new DruidExpression("", Origin.DERIVED);
                }
            }
        }
        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unknown logical binary: " + call);
    }

    private DruidExpression handleBetween(
            CallExpression between,
            Function<VariableReferenceExpression, Selection> context)
    {
        if (between.getArguments().size() == 3) {
            String name = between.getArguments().get(0).accept(this, context).getDefinition();
            if (!name.equals(__TIME)) {
                return new DruidExpression("", Origin.DERIVED);
            }
            RowExpression min = between.getArguments().get(1);
            RowExpression max = between.getArguments().get(2);
            String start = null;
            String end = null;

            if (min instanceof ConstantExpression) {
                ConstantExpression ce = (ConstantExpression) min;
                start = convertForIso8601(ce);
            }
            if (max instanceof ConstantExpression) {
                ConstantExpression ce = (ConstantExpression) max;
                end = convertForIso8601(ce);
            }
            if (start == null || end == null) {
                return new DruidExpression("", Origin.DERIVED);
            }

            return derived(format(
                "(%s %s '%s' AND %s %s '%s')",
                START_SEG,
                ">=",
                start,
                END_SEG,
                "<=",
                end));
        }

        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Between operator not supported: " + between);
    }

    private DruidExpression handleNot(CallExpression not, Function<VariableReferenceExpression, Selection> context)
    {
        return new DruidExpression("", Origin.DERIVED);
    }

    private DruidExpression handleCast(CallExpression cast, Function<VariableReferenceExpression, Selection> context)
    {
        if (cast.getArguments().size() == 1) {
            RowExpression input = cast.getArguments().get(0);
            Type expectedType = cast.getType();
            if (typeManager.canCoerce(input.getType(), expectedType)) {
                return input.accept(this, context);
            }
            throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Non implicit casts not supported: " + cast);
        }

        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "This type of CAST operator not supported: " + cast);
    }

    @Override
    public DruidExpression visitCall(CallExpression call, Function<VariableReferenceExpression, Selection> context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            return handleNot(call, context);
        }
        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            return handleCast(call, context);
        }
        if (standardFunctionResolution.isBetweenFunction(functionHandle)) {
            return handleBetween(call, context);
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isArithmeticOperator()) {
                throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Arithmetic expressions are not supported in Druid filter: " + call);
            }
            if (operatorType.isComparisonOperator()) {
                return handleLogicalBinary(operatorType.getOperator(), call, context);
            }
        }

        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Function " + call + " not supported in Druid filter");
    }

    @Override
    public DruidExpression visitInputReference(InputReferenceExpression reference, Function<VariableReferenceExpression, Selection> context)
    {
        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Druid does not support struct dereference: " + reference);
    }

    @Override
    public DruidExpression visitConstant(ConstantExpression literal, Function<VariableReferenceExpression, Selection> context)
    {
        return new DruidExpression(getLiteralAsString(session, literal), Origin.LITERAL);
    }

    @Override
    public DruidExpression visitLambda(LambdaDefinitionExpression lambda, Function<VariableReferenceExpression, Selection> context)
    {
        throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Druid does not support lambda: " + lambda);
    }

    @Override
    public DruidExpression visitVariableReference(VariableReferenceExpression reference, Function<VariableReferenceExpression, Selection> context)
    {
        // Resolve variable names.
        Selection input = requireNonNull(context.apply(reference), format("Input column %s does not exist in the input: %s", reference, context));
        return new DruidExpression(input.getEscapedDefinition(), input.getOrigin());
    }

    @Override
    public DruidExpression visitSpecialForm(SpecialFormExpression specialForm, Function<VariableReferenceExpression, Selection> context)
    {
        switch (specialForm.getForm()) {
            case IF:
            case NULL_IF:
            case SWITCH:
            case WHEN:
            case IS_NULL:
            case COALESCE:
            case DEREFERENCE:
            case ROW_CONSTRUCTOR:
            case BIND:
                throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Druid does not support special form: " + specialForm);
            case IN:
                return handleIn(specialForm, true, context);
            case AND:
            case OR:
                return handleAndOr(specialForm, context);
            default:
                throw new PrestoException(DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Druid does not support special form: " + specialForm);
        }
    }

    private DruidExpression handleAndOr(SpecialFormExpression specialForm, Function<VariableReferenceExpression, Selection> context)
    {
        RowExpression leftSide = specialForm.getArguments().get(0);
        RowExpression rightSide = specialForm.getArguments().get(1);
        DruidExpression leftDe = new DruidExpression("", Origin.DERIVED);
        DruidExpression rightDe = new DruidExpression("", Origin.DERIVED);

        if (leftSide instanceof CallExpression) {
            leftDe = visitCall((CallExpression) leftSide, context);
        }
        if (rightSide instanceof CallExpression) {
            rightDe = visitCall((CallExpression) rightSide, context);
        }
        if (leftDe.getDefinition().isEmpty() && rightDe.getDefinition().isEmpty()) {
            return new DruidExpression("", Origin.DERIVED);
        }
        if (leftDe.getDefinition().isEmpty()) {
            return rightDe;
        }
        if (rightDe.getDefinition().isEmpty()) {
            return leftDe;
        }

        return derived(format(
                "(%s %s %s)",
                leftDe.getDefinition(),
                specialForm.getForm().toString(),
                rightDe.getDefinition()));
    }
}
