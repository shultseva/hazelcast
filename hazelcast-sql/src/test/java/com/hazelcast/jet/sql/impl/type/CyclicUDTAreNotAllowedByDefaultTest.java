/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.type;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.A;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.B;
import com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.C;
import com.hazelcast.map.IMap;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.sql.impl.type.BasicNestedFieldsTest.createJavaMapping;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CyclicUDTAreNotAllowedByDefaultTest extends SqlTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initializeWithClient(1, null, null);
    }

    @Test
    public void test_defaultBehaviorFailsOnCycles() {
        createType("AType", "name VARCHAR", "b BType");
        createType("BType", "name VARCHAR", "c CType");
        createType("CType", "name VARCHAR", "a AType");

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;
        createType("SimpleType", "name VARCHAR");

        createJavaMapping(client(), "cycled_table", A.class, "this AType");
        createJavaMapping(client(), "plain_table", SimpleType.class, "this SimpleType");

        IMap<Long, A> map = client().getMap("cycled_table");
        map.put(1L, a);

        assertThatThrownBy(() -> instance().getSql().execute("SELECT * FROM cycled_table"))
                .hasMessageContaining("Experimental feature of using cyclic custom types isn't enabled.");
    }

    // TODO [sasha]: collect all duplicated usages and move to SqlTestSupport
    private static void createType(String name, String... fields) {
        new SqlType(name)
                .fields(fields)
                .create(client());
    }

    @Test
    public void test_defaultJoinBehaviorFailsOnCycles() {
        createType("AType", "name VARCHAR", "b BType");
        createType("BType", "name VARCHAR", "c CType");
        createType("CType", "name VARCHAR", "a AType");


        createType("SimpleType", "name VARCHAR");

        final A a = new A("a");
        final B b = new B("b");
        final C c = new C("c");

        a.b = b;
        b.c = c;
        c.a = a;

        createJavaMapping(client(), "cycled_table", A.class, "this AType");
        createJavaMapping(client(), "plain_table1", SimpleType.class, "this SimpleType");
        createJavaMapping(client(), "plain_table2", SimpleType.class, "this SimpleType");
        createJavaMapping(client(), "plain_table3", SimpleType.class, "this SimpleType");


        IMap<Long, A> map = client().getMap("cycled_table");
        map.put(1L, a);

        assertThatThrownBy(() ->
                instance().getSql().execute("SELECT * FROM plain_table1 as t1 " +
                        "JOIN plain_table2 as t2 on t2.this.name = t1.this.name " +
                        "JOIN cycled_table as t3 on t3.this.name = t2.this.name " +
                        "JOIN (SELECT t4.this.name as new_name from plain_table3 as t4) as j on j.new_name = t2.this.name " +
                        "WHERE t2.this.name != 'foo' AND t1.this.name = 'bar' " +
                        "ORDER BY t1.this.name LIMIT 100"))
                .hasMessageContaining("Experimental feature of using cyclic custom types isn't enabled.");
    }

    static class SimpleType {
        String name;

        SimpleType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
