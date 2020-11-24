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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestDruidSegmentExpressionConverters
        extends TestDruidQueryBase
{
    private final Function<VariableReferenceExpression, DruidQueryGeneratorContext.Selection> testInputFunction = testInput::get;

    @Test
    public void testProjectExpressionConverter()
    {
        SessionHolder sessionHolder = new SessionHolder();
        testProject("secondssinceepoch", "secondsSinceEpoch", sessionHolder);
    }

    private void testProject(String sqlExpression, String expectedDruidExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualDruidExpression = pushDownExpression.accept(new DruidProjectExpressionConverter(
                        functionAndTypeManager,
                        standardFunctionResolution),
                testInput).getDefinition();
        assertEquals(actualDruidExpression, expectedDruidExpression);
    }

    @Test
    public void testFilterExpressionConverter()
    {
        SessionHolder sessionHolder = new SessionHolder();

        // Greater than/Less than
        testFilter("__time > timestamp '2016-06-26 19:00:00.000 UTC'", "(\"start\" >= '2016-06-26T19:00:00.000Z')", sessionHolder);
        testFilter("__time >= timestamp '2016-06-26 19:00:00.000 UTC'", "(\"start\" >= '2016-06-26T19:00:00.000Z')", sessionHolder);
        testFilter("__time < timestamp '2016-06-26 19:00:00.000 UTC'", "(\"end\" <= '2016-06-26T19:00:00.000Z')", sessionHolder);
        testFilter("__time <= timestamp '2016-06-26 19:00:00.000 UTC'", "(\"end\" <= '2016-06-26T19:00:00.000Z')", sessionHolder);

        // Equals
        testFilter("__time = timestamp '2016-06-26 19:00:00.000 UTC'",
                "(\"start\" >= '2016-06-26T19:00:00.000Z' AND \"end\" <= '2016-06-26T19:00:00.000Z')", sessionHolder);

        // Between
        testFilter("__time between timestamp '2016-06-26 19:00:00.000 UTC' AND timestamp '2016-06-27 00:00:00.000 UTC'",
                "(\"start\" >= '2016-06-26T19:00:00.000Z' AND \"end\" <= '2016-06-27T00:00:00.000Z')", sessionHolder);

        // In clause
        testFilter(
                "__time IN (timestamp '2016-06-26 19:00:00.000 UTC', timestamp '2016-06-27 00:00:00.000 UTC')",
                "(\"start\" >= '2016-06-26T19:00:00.000Z' AND \"end\" <= '2016-06-26T19:00:00.000Z') OR (\"start\" >= '2016-06-27T00:00:00.000Z' AND \"end\" <= '2016-06-27T00:00:00.000Z')",
                sessionHolder);

        // And/Or
        testFilter(
                "__time > timestamp '2016-06-26 19:00:00.000 UTC' AND __time < timestamp '2016-06-27 19:00:00.000 UTC'",
                "((\"start\" >= '2016-06-26T19:00:00.000Z') AND (\"end\" <= '2016-06-27T19:00:00.000Z'))",
                sessionHolder);

        // Should return empty
        testFilter("totalfare between 20 and 30 AND \"region.id\" > 20 OR city = 'Campbell'", "", sessionHolder);
        testFilter("\"region.id\" = 20", "", sessionHolder);
        testFilter("secondssinceepoch > 1559978258", "", sessionHolder);
    }

    private void testFilter(String sqlExpression, String expectedDruidExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualDruidExpression = pushDownExpression.accept(new DruidSegmentFilterExpressionConverter(
                        functionAndTypeManager,
                        functionAndTypeManager,
                        standardFunctionResolution,
                        sessionHolder.getConnectorSession()),
                testInputFunction).getDefinition();
        assertEquals(actualDruidExpression, expectedDruidExpression);
    }

    private void testFilterUnsupported(String sqlExpression, SessionHolder sessionHolder)
    {
        try {
            RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
            String actualDruidExpression = pushDownExpression.accept(new DruidFilterExpressionConverter(
                            functionAndTypeManager,
                            functionAndTypeManager,
                            standardFunctionResolution,
                            sessionHolder.getConnectorSession()),
                    testInputFunction).getDefinition();
            fail("expected to not reach here: Generated " + actualDruidExpression);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), DRUID_PUSHDOWN_UNSUPPORTED_EXPRESSION.toErrorCode());
        }
    }
}
