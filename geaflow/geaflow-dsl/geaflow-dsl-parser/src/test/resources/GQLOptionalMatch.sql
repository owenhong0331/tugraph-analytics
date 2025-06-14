/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


OPTIONAL MATCH (a:person where id = 1)-[e:knows where e.weight > 0.4]->(b:person where id = 1) RETURN a;

-- 无标签顶点带反向关系和条件
OPTIONAL MATCH (a WHERE name = 'marko')<-[e]-(b) WHERE a.name <> b.name RETURN e;

-- 多标签顶点和双向关系
OPTIONAL MATCH (a:person|animal|device WHERE name = 'where')-[e]-(b) RETURN b;

-- 仅返回属性
OPTIONAL MATCH (a WHERE name = 'match')-[e]->(b) RETURN a.name;

-- 关系属性访问
OPTIONAL MATCH (a WHERE name = 'knows')<-[e]->(b) RETURN e.test;

-- 多模式组合 (逗号分隔)
OPTIONAL MATCH (a)->(b) - (c), (a) -> (d) <- (f) RETURN a, b;
OPTIONAL MATCH (a)<-(b) <->(c), (c) -> (d) - (f) RETURN b, c, f;



