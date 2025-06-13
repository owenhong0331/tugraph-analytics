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
set geaflow.dsl.window.size = 1;
set geaflow.dsl.ignore.exception = true;

-- CREATE GRAPH IF NOT EXISTS dy_modern (
--     Vertex person (
--         id bigint ID,
--         name varchar
--     ),
--     Edge knows (
--         srcId bigint SOURCE ID,
--         targetId bigint DESTINATION ID,
--         weight double
--     )
-- ) WITH (
--     storeType='rocksdb',
--     shardCount = 1
-- );

-- CREATE TABLE IF NOT EXISTS tbl_source (
--     text varchar
-- ) WITH (
--     type='file',
--     `geaflow.dsl.file.path` = 'resource:///demo/demo_job_data.txt',
--     `geaflow.dsl.column.separator`='|'
-- );
CREATE TABLE v_person (
  name varchar,
  age int,
  personId bigint
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_vertex_person_reorder.txt'
);

CREATE TABLE e_knows (
  knowsSrc bigint,
  knowsTarget bigint,
  weight double
) WITH (
	type='file',
	geaflow.dsl.window.size = -1,
	geaflow.dsl.file.path = 'resource:///data/modern_edge_knows.txt'
);

CREATE TABLE IF NOT EXISTS tbl_result (
    f1 varchar,
    f2 varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

CREATE GRAPH dy_modern(
    Vertex person using v_person WITH ID(personId),
    Vertex person2 using v_person WITH ID(personId),
	Edge knows using e_knows WITH ID(knowsSrc, knowsTarget)
) WITH (
	storeType='memory',
	shardCount = 2
);

USE GRAPH dy_modern;

-- INSERT INTO dy_modern.person(id, name)
-- SELECT
--     cast(trim(split_ex(t1, ',', 0)) as bigint),
--     split_ex(trim(t1), ',', 1)
-- FROM (
--     Select trim(substr(text, 2)) as t1
--     FROM tbl_source
--     WHERE substr(text, 1, 1) = ','
-- );

-- INSERT INTO dy_modern.knows
-- SELECT
--     cast(split_ex(t1, ',', 0) as bigint),
--     cast(split_ex(t1, ',', 1) as bigint),
--     cast(split_ex(t1, ',', 2) as double)
-- FROM (
--     Select trim(substr(text, 2)) as t1
--     FROM tbl_source
--     WHERE substr(text, 1, 1) = '-'
-- );



-- 以Pattern 1为例
-- 其他Pattern按照需要修改GQL即可
INSERT INTO tbl_result
SELECT
    p_name,
    c_name
FROM (
    optional MATCH (p:person)-[r:knows]->(c:person2)
    RETURN p.name as p_name, c.name as c_name
);



-- INSERT INTO tbl_result (f1, f2)
-- SELECT
--     p.name AS p_name,
--     k.name AS c_name
--     -- k.knowsTarget
-- FROM
--     person p
--     person2 c
-- LEFT JOIN
--     knows k ON p.personId = k.knowsSrc
-- LEFT JOIN
--     person c ON k.knowsTarget = c.personId
-- ;



-- CREATE TABLE tbl_result (
--   a_id bigint,
--   srcId bigint,
--   targetId bigint
-- ) WITH (
-- 	type='file',
-- 	geaflow.dsl.file.path='${target}'
-- );

-- USE GRAPH modern;

-- INSERT INTO tbl_result
-- SELECT
-- 	a_id,
--   srcId,
--   targetId
-- FROM (
--   OPTIONAL MATCH (a) <-[e:knows]-(b:person)
--   RETURN a.id as a_id, e.srcId, e.targetId
-- )
