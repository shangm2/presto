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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.OrderBy;
import com.google.common.collect.Streams;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.createSymbolReference;
import static com.facebook.presto.sql.planner.PlannerUtils.toSortOrder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AggregationFunctionMatcher
        implements RvalueMatcher
{
    private final ExpectedValueProvider<FunctionCall> callMaker;
    private final Optional<Symbol> mask;

    public AggregationFunctionMatcher(ExpectedValueProvider<FunctionCall> callMaker)
    {
        this.callMaker = requireNonNull(callMaker, "functionCall is null");
        this.mask = Optional.empty();
    }

    public AggregationFunctionMatcher(ExpectedValueProvider<FunctionCall> callMaker, Symbol mask)
    {
        this.callMaker = requireNonNull(callMaker, "functionCall is null");
        this.mask = Optional.of(requireNonNull(mask, "mask is null"));
    }

    @Override
    public Optional<VariableReferenceExpression> getAssignedVariable(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<VariableReferenceExpression> result = Optional.empty();
        if (!(node instanceof AggregationNode)) {
            return result;
        }

        AggregationNode aggregationNode = (AggregationNode) node;

        FunctionCall expectedCall = callMaker.getExpectedValue(symbolAliases);
        for (Map.Entry<VariableReferenceExpression, Aggregation> assignment : aggregationNode.getAggregations().entrySet()) {
            if (verifyAggregation(metadata.getFunctionAndTypeManager(), assignment.getValue(), expectedCall, mask.map(x -> new Symbol(symbolAliases.get(x.getName()).getName())))) {
                checkState(!result.isPresent(), "Ambiguous function calls in %s", aggregationNode);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private static boolean verifyAggregation(FunctionAndTypeManager functionAndTypeManager, Aggregation aggregation, FunctionCall expectedCall, Optional<Symbol> mask)
    {
        return functionAndTypeManager.getFunctionMetadata(aggregation.getFunctionHandle()).getName().getObjectName().equalsIgnoreCase(expectedCall.getName().getSuffix()) &&
                aggregation.getArguments().size() == expectedCall.getArguments().size() &&
                Streams.zip(
                        aggregation.getArguments().stream(),
                        expectedCall.getArguments().stream(),
                        (actualArgument, expectedArgument) -> isEquivalent(Optional.of(expectedArgument), Optional.of(actualArgument))).allMatch(Boolean::booleanValue) &&
                isEquivalent(expectedCall.getFilter(), aggregation.getFilter()) &&
                expectedCall.isDistinct() == aggregation.isDistinct() &&
                verifyAggregationOrderBy(aggregation.getOrderBy(), expectedCall.getOrderBy()) &&
                maskMatch(mask, aggregation.getMask());
    }

    private static boolean verifyAggregationOrderBy(Optional<OrderingScheme> orderingScheme, Optional<OrderBy> expectedSortOrder)
    {
        if (orderingScheme.isPresent() && expectedSortOrder.isPresent()) {
            return verifyAggregationOrderBy(orderingScheme.get(), expectedSortOrder.get());
        }
        return orderingScheme.isPresent() == expectedSortOrder.isPresent();
    }

    private static boolean verifyAggregationOrderBy(OrderingScheme orderingScheme, OrderBy expectedSortOrder)
    {
        if (orderingScheme.getOrderByVariables().size() != expectedSortOrder.getSortItems().size()) {
            return false;
        }
        for (int i = 0; i < expectedSortOrder.getSortItems().size(); i++) {
            VariableReferenceExpression orderingVariable = orderingScheme.getOrderByVariables().get(i);
            if (expectedSortOrder.getSortItems().get(i).getSortKey().equals(createSymbolReference(orderingVariable)) &&
                    toSortOrder(expectedSortOrder.getSortItems().get(i)).equals(orderingScheme.getOrdering(orderingVariable))) {
                continue;
            }
            return false;
        }
        return true;
    }

    private static boolean isEquivalent(Optional<Expression> expression, Optional<RowExpression> rowExpression)
    {
        // Function's argument provided by FunctionCallProvider is SymbolReference that already resolved from symbolAliases.
        if (rowExpression.isPresent() && expression.isPresent() && !(expression.get() instanceof AnySymbolReference)) {
            checkArgument(rowExpression.get() instanceof VariableReferenceExpression, "can only process variableReference: " + rowExpression.get());
            return expression.get().equals(createSymbolReference(((VariableReferenceExpression) rowExpression.get())));
        }
        return rowExpression.isPresent() == expression.isPresent();
    }

    private static boolean maskMatch(Optional<Symbol> symbol, Optional<VariableReferenceExpression> mask)
    {
        if (symbol.isPresent() && mask.isPresent()) {
            return symbol.get().getName().equals(mask.get().getName());
        }
        return symbol.isPresent() == mask.isPresent();
    }

    @Override
    public String toString()
    {
        return callMaker.toString();
    }
}
