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

CREATE TABLE tbl_result (
  person_id bigint,
  person_name varchar,
  friend_id bigint,
  friend_name varchar
) WITH (
	type='file',
	geaflow.dsl.file.path='${target}'
);

USE GRAPH modern;

-- Test OPTIONAL MATCH: should return all persons, with null for those without friends
INSERT INTO tbl_result
SELECT
	person_id,
	person_name,
	friend_id,
	friend_name
FROM (
  MATCH (p:person)
  OPTIONAL MATCH (p)-[e:knows]->(f:person)
  RETURN p.id as person_id, p.name as person_name, f.id as friend_id, f.name as friend_name
);
