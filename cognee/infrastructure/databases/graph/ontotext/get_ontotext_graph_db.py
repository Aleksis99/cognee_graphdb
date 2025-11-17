import requests
import re
import json
from typing import Optional, Dict, Any, List, Tuple, Type, Union
from uuid import UUID
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from cognee.infrastructure.databases.graph.graph_db_interface import (
    GraphDBInterface,
    NodeData,
    Node,
    EdgeData,
)
from urllib.parse import quote, urlparse
from cognee.infrastructure.engine import DataPoint
from cognee.modules.storage.utils import JSONEncoder
from rfc3987 import parse


def uri_to_key(uri) -> str:
    uri_str = str(uri)
    if "#" in uri_str:
        name = uri_str.split("#")[-1]
    elif ":" in uri_str:
        name = uri_str.split(":")[-1]
    elif "//" in uri_str:
        name = uri_str.rstrip("/").split("/")[-1]

    return name.lower().replace(" ", "_").strip()


def is_iri(string: str):
    try:
        return parse(string, rule="IRI")
    except ValueError:
        return False


def is_uri(string: str):
    try:
        return parse(string, rule="IRI")
    except Exception:
        return False


def to_uri(string: str, prefix: str = "http://example.org/", suffix=""):
    if is_uri(string):
        return string
    return f"{prefix}{suffix}{string}"


def to_quoted_uri(uri: Any) -> Any:
    try:
        return quote(uri)
    except Exception as e:
        return uri


class OntotextGraphDB(GraphDBInterface):
    def __init__(
        self,
        endpoint: str,
        repository: str = "test",
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        print("REPO", repository)
        self.update_endpoint = f"{endpoint}/repositories/{repository}/statements"
        self.sparql = SPARQLWrapper(f"{endpoint}/repositories/{repository}")
        self.auth = (username, password) if username and password else None
        if username and password:
            self.sparql.setCredentials(username, password)
        self.sparql.setReturnFormat(JSON)
        self.sparql.setTimeout(30)

    async def is_property(self, relation_uri: str) -> bool:
        """
        Check if a relation/predicate is a property in GraphDB.

        Args:
            relation_uri (str): The URI of the relation to check
                               (e.g., 'http://example.org/hasProperty')

        Returns:
            bool: True if the relation is a property (rdf:Property, owl:ObjectProperty,
                  owl:DatatypeProperty, or owl:AnnotationProperty), False otherwise
        """
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        ASK {{
            <{relation_uri}> a ?type .
            FILTER (?type IN (rdf:Property, owl:ObjectProperty, owl:DatatypeProperty, 
                              owl:AnnotationProperty, rdfs:Property))
        }}
        """

        self.sparql.setQuery(query)

        try:
            results = self.sparql.query().convert()
            return results.get("boolean", False)
        except Exception as e:
            print(f"Error querying GraphDB: {e}")
            return False

    async def query(self, query: str, params: dict) -> List[Any]:
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        return results["results"]["bindings"]

    def _construct_insert_query(self, triples: List[Tuple[str, str, str]]) -> str:
        query = "INSERT DATA { "
        for subject, predicate, obj in triples:
            # subject = to_uri(subject)

            if predicate == "a":
                subject = to_uri(subject, suffix="urn:node:")
                obj = to_uri(obj, suffix="urn:node:")
                query += f"<{subject}> a <{obj}> . "
            elif subject.startswith("<<"):
                predicate = to_uri(predicate, suffix="urn:relationship:")
                query += f"{subject} <{predicate}> {obj} . "
            elif obj.startswith("urn:") or is_uri(obj):
                subject = to_uri(subject, suffix="urn:node:")
                obj = to_uri(obj, suffix="urn:node:")
                predicate = to_uri(predicate, suffix="urn:relationship:")
                query += f"<{subject}> <{predicate}> <{obj}> . "
            else:
                subject = to_uri(subject, suffix="urn:node:")
                predicate = to_uri(predicate, suffix="urn:property:")
                query += f"<{subject}> <{predicate}> {obj} . "
        query += "}"
        return query

    def _format_literal(self, value: Any) -> str:
        if isinstance(value, bool):
            return f'"{str(value).lower()}"^^<http://www.w3.org/2001/XMLSchema#boolean>'
        elif isinstance(value, int):
            return f'"{value}"^^<http://www.w3.org/2001/XMLSchema#integer>'
        elif isinstance(value, float):
            return f'"{value}"^^<http://www.w3.org/2001/XMLSchema#double>'

        elif isinstance(value, str) and value.startswith("urn:"):
            return to_uri(value)
        # Basic escaping for string literals
        escaped_value = (
            str(value)
            .replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\r", "\\r")
            .replace("\n", "\\n")
        )
        return f'"""{escaped_value}"""'

    def serialize_properties(self, properties=dict()):
        """
        Convert property values to a suitable representation for storage.

        Parameters:
        -----------

            - properties: A dictionary of properties to serialize. (default dict())

        Returns:
        --------

            A dictionary of serialized properties.
        """
        serialized_properties = {}

        for property_key, property_value in properties.items():
            if isinstance(property_value, UUID):
                serialized_properties[property_key] = str(property_value)
                continue

            if isinstance(property_value, dict):
                serialized_properties[property_key] = json.dumps(
                    property_value, cls=JSONEncoder
                )
                continue

            serialized_properties[property_key] = property_value

        return serialized_properties

    async def add_node(
        self, node: Union[DataPoint, str], properties: Optional[Dict[str, Any]] = None
    ) -> None:

        if not isinstance(node, str):
            serialized_properties = self.serialize_properties(node.model_dump())
            node_id = str(node.id)
            properties = serialized_properties
        else:
            node_id = node
            properties = properties or {}

        # triples = [(f"urn:node:{node_id}", "a", "urn:schema:node")]
        if properties:
            for key, value in properties.items():
                if value is None:
                    continue
                triples.append(
                    (
                        node_id,
                        key,
                        self._format_literal(value),
                    )
                )
        if triples:
            query = self._construct_insert_query(triples)
            response = requests.post(
                self.update_endpoint,
                data={"update": query},
                auth=self.auth,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()

    async def add_nodes(self, nodes: Union[List[Node], List[DataPoint]]) -> None:
        triples = []
        for node in nodes:
            if hasattr(node, "id") and hasattr(node, "model_dump"):
                node_id = str(node.id)
                properties = self.serialize_properties(node.model_dump())
            else:
                node_id, properties = node

            # triples.append((f"urn:node:{node_id}", "a", "urn:schema:node"))
            for key, value in properties.items():
                if value is None:
                    continue
                triples.append(
                    (
                        node_id,
                        key,
                        self._format_literal(value),
                    )
                )

        if triples:
            query = self._construct_insert_query(triples)
            print(query)
            response = requests.post(
                self.update_endpoint,
                data={"update": query},
                auth=self.auth,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()

    async def delete_node(self, node_id: str) -> None:
        query = f"""
            DELETE {{
                ?node ?p_out ?o .
                ?s ?p_in ?node .
            }}
            WHERE {{
                {{
                    ?node ?p_out ?o .
                    FILTER(isIRI(?node) && CONTAINS(STR(?node), "{node_id}"))
                }}
                UNION
                {{
                    ?s ?p_in ?node .
                    FILTER(isIRI(?node) && CONTAINS(STR(?node), "{node_id}"))
                }}
            }}
        """
        response = requests.post(
            self.update_endpoint,
            data={"update": query},
            auth=self.auth,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()

    async def delete_nodes(self, node_ids: List[str]) -> None:
        if not node_ids:
            return
        filter_parts = [f'CONTAINS(STR(?node), "{nid}")' for nid in node_ids]
        filter_str = " || ".join(filter_parts)
        query = f"""
            DELETE {{
                ?node ?p_out ?o .
                ?s ?p_in ?node .
            }}
            WHERE {{
                {{
                    ?node ?p_out ?o .
                    FILTER(isIRI(?node) && ({filter_str}))
                }}
                UNION
                {{
                    ?s ?p_in ?node .
                    FILTER(isIRI(?node) && ({filter_str}))
                }}
            }}
        """
        response = requests.post(
            self.update_endpoint,
            data={"update": query},
            auth=self.auth,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()

    async def get_node(self, node_id: str) -> Optional[NodeData]:
        query = f"""
            SELECT ?p ?o
            WHERE {{
                ?node ?p ?o .

                FILTER(
                    isIRI(?node) && 
                    CONTAINS(STR(?node), "{node_id}")
                )
            }}
        """
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        if not results["results"]["bindings"]:
            return None

        properties = {}
        for result in results["results"]["bindings"]:
            predicate = result["p"]["value"].split("urn:property:")[1]
            obj = result["o"]["value"]
            properties[predicate] = obj
        # properties["node_id"] = node_id
        return properties

    async def get_nodes(self, node_ids: List[str]) -> List[NodeData]:
        if not node_ids:
            return []

        regex_str = "|".join(re.escape(nid) for nid in node_ids)
        query = f"""
            SELECT ?node ?p ?o
            WHERE {{
                ?node ?p ?o .
                FILTER(isIRI(?node) && REGEX(STR(?node), "{regex_str}"))
            }}
        """
        self.sparql.setQuery(query)
        self.sparql.setMethod(POST)
        results = self.sparql.query().convert()

        nodes = {node_id: {} for node_id in node_ids}
        for result in results["results"]["bindings"]:
            node_id = result["node"]["value"].split("urn:node:")[1]
            predicate = result["p"]["value"].split("urn:property:")[1]
            obj = result["o"]["value"]
            nodes[node_id][predicate] = obj

        return [properties for properties in nodes.values() if properties]

    async def add_edge(
        self,
        source_id: str,
        target_id: str,
        relationship_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        source_uri = to_uri(source_id, suffix="urn:node:")
        target_uri = to_uri(target_id, suffix="urn:node:")
        rel_uri = to_uri(relationship_name, suffix="urn:relationship:")
        triples = [
            (
                source_uri,
                rel_uri,
                target_uri,
            )
        ]
        edge_props = properties or {}
        serialized_properties = self.serialize_properties(edge_props)
        # triples.append(
        #     (
        #         f"urn:relationship:{source_id}_{target_id}",
        #         "a",
        #         ":relationship",
        #     )
        # )
        for prop_key, prop_value in serialized_properties.items():
            triples.append(
                (
                    f"<< <{source_uri}> <{rel_uri}> <{target_uri}> >>",
                    prop_key,
                    self._format_literal(prop_value),
                )
            )
        query = self._construct_insert_query(triples)
        response = requests.post(
            self.update_endpoint,
            data={"update": query},
            auth=self.auth,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()

    async def add_edges(
        self,
        edges: Union[
            List[EdgeData], List[Tuple[str, str, str, Optional[Dict[str, Any]]]]
        ],
    ) -> None:
        triples = []
        for edge in edges:
            source_id, target_id, relationship_name, properties = edge
            source_uri = to_uri(source_id, suffix="urn:node:")
            target_uri = to_uri(target_id, suffix="urn:node:")
            rel_uri = to_uri(relationship_name, suffix="urn:relationship:")
            triples = [
                (
                    source_uri,
                    rel_uri,
                    target_uri,
                )
            ]
            edge_props = properties or {}
            serialized_properties = self.serialize_properties(edge_props)
            # triples.append(
            #     (
            #         f"urn:relationship:{source_id}_{target_id}",
            #         "a",
            #         ":relationship",
            #     )
            # )
            for prop_key, prop_value in serialized_properties.items():
                triples.append(
                    (
                        f"<< <{source_uri}> <{rel_uri}> <{target_uri}> >>",
                        prop_key,
                        self._format_literal(prop_value),
                    )
                )

        if triples:
            query = self._construct_insert_query(triples)
            print("add edges query\n", query)
            with open("test.rq", "w", encoding="utf-8") as f:
                f.write(query)
            response = requests.post(
                self.update_endpoint,
                data={"update": query},
                auth=self.auth,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()

    async def delete_graph(self) -> None:
        query = "CLEAR ALL"
        response = requests.post(
            self.update_endpoint,
            data={"update": query},
            auth=self.auth,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()

    def get_relation_properties(self, rel_id) -> Dict[str, Any]:
        query = f""" PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT ?property ?value
            WHERE {{
              {rel_id} ?property ?value .
            }}"""
        try:

            self.sparql.setQuery(query)
            results = self.sparql.query().convert()
            properties = {}

            for result in results["results"]["bindings"]:
                prop = uri_to_key(result["property"]["value"])
                value = result["value"]["value"]
                properties[prop] = value
            return properties
        except Exception as e:
            print(f"Error getting properties {e}")
        return {}

    async def get_graph_data(self) -> Tuple[List[Node], List[EdgeData]]:
        query = "SELECT DISTINCT ?s ?p ?o WHERE { ?s ?p ?o .}"
        self.sparql.setQuery(query)
        self.sparql.setMethod(POST)
        results = self.sparql.query().convert()

        nodes = {}
        edges = []
        properties = set()
        rel2properties = {}

        for result in results["results"]["bindings"]:
            subject = result["s"]["value"]
            predicate = result["p"]["value"]
            obj = result["o"]["value"]
            # if await self.is_property(subject):
            #     continue
            if "urn:node:" in subject or is_uri(subject) or subject.startswith(":"):
                node_id = uri_to_key(subject)
                if node_id not in nodes:
                    nodes[node_id] = {}

                if (
                    "urn:property:" in predicate
                    or not is_uri(obj)
                    or predicate in properties
                    # or await self.is_property(predicate)
                ):
                    properties.add(predicate)
                    prop_name = uri_to_key(predicate)
                    nodes[node_id][prop_name] = obj
                else:
                    rel_name = uri_to_key(predicate)
                    if "urn:node:" in obj:
                        target_id = obj.split("urn:node:")[1]
                    else:
                        target_id = uri_to_key(obj)
                    rel_id = None
                    node_id_obj = uri_to_key(obj)
                    if is_uri(obj):
                        rel_id = f"<< <{subject}> <{predicate}> <{obj}> >>"
                        if rel_id not in rel2properties:
                            rel2properties[rel_id] = self.get_relation_properties(
                                rel_id
                            )

                    edges.append(
                        (
                            node_id,
                            target_id,
                            rel_name,
                            rel2properties[rel_id] if rel_id is not None else {},
                        )
                    )
                    if node_id_obj not in nodes:
                        nodes[node_id_obj] = {}

        node_list = [(node_id, properties) for node_id, properties in nodes.items()]
        # print(node_list)
        # print(edges)
        return node_list, edges

    async def get_graph_metrics(self, include_optional: bool = False) -> Dict[str, Any]:
        node_count_query = "SELECT (COUNT(DISTINCT ?s) as ?node_count) WHERE { ?s a <http://example.org/urn:schema:node> . }"
        self.sparql.setQuery(node_count_query)
        results = self.sparql.query().convert()
        node_count = int(results["results"]["bindings"][0]["node_count"]["value"])

        edge_count_query = 'SELECT (COUNT(?s) as ?edge_count) WHERE { ?s ?p ?o . FILTER(CONTAINS(STR(?p), "urn:relationship:")) }'
        self.sparql.setQuery(edge_count_query)
        results = self.sparql.query().convert()
        edge_count = int(results["results"]["bindings"][0]["edge_count"]["value"])

        return {
            "node_count": node_count,
            "edge_count": edge_count,
        }

    async def has_edge(
        self, source_id: str, target_id: str, relationship_name: str
    ) -> bool:
        source_id, target_id, relationship_name = (
            to_quoted_uri(source_id),
            to_quoted_uri(target_id),
            to_quoted_uri(relationship_name),
        )
        query = f"""
            ASK WHERE {{
                <http://example.org/urn:node:{source_id}> <http://example.org/urn:relationship:{relationship_name}> <http://example.org/urn:node:{target_id}> .
            }}
        """
        # print(query)
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        return results["boolean"]

    async def has_edges(self, edges: List[EdgeData]) -> List[EdgeData]:
        if not edges:
            return []

        values_clauses = []
        for edge in edges:
            if len(edge) == 4:
                source_id, target_id, relationship_name, _ = edge
            else:
                source_id, target_id, relationship_name = edge
            source_id, target_id, relationship_name = (
                to_quoted_uri(source_id),
                to_quoted_uri(target_id),
                to_quoted_uri(relationship_name),
            )
            if relationship_name == "a":
                values_clauses.append(
                    f"(<http://example.org/urn:node:{source_id}> a <http://example.org/urn:schema:{target_id}>)"
                )
            else:
                values_clauses.append(
                    f"(<http://example.org/urn:node:{source_id}> <http://example.org/urn:relationship:{relationship_name}> <http://example.org/urn:node:{target_id}>)"
                )

        values_string = " ".join(values_clauses)
        query = f"""
            SELECT ?source ?relationship ?target
            WHERE {{
                VALUES (?source ?relationship ?target) {{
                    {values_string}
                }}
                ?source ?relationship ?target .
            }}
        """
        print(query)
        print("=" * 50)
        self.sparql.setQuery(query)
        self.sparql.setMethod(POST)
        results = self.sparql.query().convert()

        existing_edges = []
        for result in results["results"]["bindings"]:
            source_uri = result["source"]["value"]
            target_uri = result["target"]["value"]
            relationship_uri = result["relationship"]["value"]

            source_id = source_uri.split("urn:node:")[1]
            target_id = target_uri.split("urn:node:")[1]
            relationship_name = relationship_uri.split("urn:relationship:")[1]

            # Find the original edge to preserve any properties
            original_edge = next(
                (
                    edge
                    for edge in edges
                    if len(edge) > 2
                    and edge[0] == source_id
                    and edge[1] == target_id
                    and edge[2] == relationship_name
                ),
                None,
            )
            if original_edge:
                existing_edges.append(original_edge)

        return existing_edges

    async def get_edges(self, node_id: str) -> List[EdgeData]:
        query = f"""
            SELECT ?p ?o
            WHERE {{
                <http://example.org/urn:node:{node_id}> ?p ?o .
                FILTER(CONTAINS(STR(?p), "urn:relationship:"))
            }}
        """
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()

        edges = []
        for result in results["results"]["bindings"]:
            predicate = result["p"]["value"].split("urn:relationship:")[1]
            target_uri = result["o"]["value"]
            target_id = target_uri.split("urn:node:")[1]
            edges.append((node_id, target_id, predicate, {}))

        return edges

    async def get_neighbors(self, node_id: str) -> List[NodeData]:
        query = f"""
            SELECT DISTINCT ?neighbor ?p ?o
            WHERE {{
                {{
                    <http://example.org/urn:node:{node_id}> ?rel ?neighbor .
                    FILTER(isIRI(?neighbor))
                }}
                UNION
                {{
                    ?neighbor ?rel <http://example.org/urn:node:{node_id}> .
                    FILTER(isIRI(?neighbor))
                }}
                ?neighbor ?p ?o .
            }}
        """
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()

        neighbors = {}
        for result in results["results"]["bindings"]:
            neighbor_uri = result["neighbor"]["value"]
            neighbor_id = neighbor_uri.split("urn:node:")[1]

            if neighbor_id not in neighbors:
                neighbors[neighbor_id] = {}

            if result["p"]["value"].startswith("urn:property:"):
                prop_name = result["p"]["value"].split("urn:property:")[1]
                neighbors[neighbor_id][prop_name] = result["o"]["value"]

        return [properties for properties in neighbors.values() if properties]

    async def get_nodeset_subgraph(
        self, node_type: Type[Any], node_name: List[str]
    ) -> Tuple[List[Tuple[int, dict]], List[Tuple[int, int, str, dict]]]:
        values_clause = " ".join(
            [f"<http://example.org/urn:node:{node_id}>" for node_id in node_name]
        )
        query = f"""
            CONSTRUCT {{ ?s ?p ?o }}
            WHERE {{
                ?s ?p ?o .
                VALUES ?s {{ {values_clause} }}
            }}
        """
        self.sparql.setQuery(query)
        self.sparql.setMethod(POST)
        results = self.sparql.query().convert()

        nodes = {}
        edges = []
        node_id_map = {}
        next_node_id = 0

        for result in results["results"]["bindings"]:
            subject = result["s"]["value"]
            predicate = result["p"]["value"]
            obj = result["o"]["value"]

            if subject.startswith("urn:node:"):
                str_node_id = subject.split("urn:node:")[1]
                if str_node_id not in node_id_map:
                    node_id_map[str_node_id] = next_node_id
                    next_node_id += 1
                int_node_id = node_id_map[str_node_id]

                if int_node_id not in nodes:
                    nodes[int_node_id] = {}

                if predicate.startswith("urn:property:"):
                    prop_name = predicate.split("urn:property:")[1]
                    nodes[int_node_id][prop_name] = obj
                elif predicate.startswith("urn:relationship:"):
                    rel_name = predicate.split("urn:relationship:")[1]
                    if obj.startswith("urn:node:"):
                        str_target_id = obj.split("urn:node:")[1]
                        if str_target_id not in node_id_map:
                            node_id_map[str_target_id] = next_node_id
                            next_node_id += 1
                        int_target_id = node_id_map[str_target_id]
                        edges.append((int_node_id, int_target_id, rel_name, {}))

        node_list = [(node_id, properties) for node_id, properties in nodes.items()]

        return node_list, edges

    async def get_connections(
        self, node_id: Union[str, UUID]
    ) -> List[Tuple[NodeData, Dict[str, Any], NodeData]]:
        query = f"""
            SELECT ?source ?relationship ?target ?source_p ?source_o ?target_p ?target_o
            WHERE {{
                {{
                    BIND(<http://example.org/urn:node:{node_id}> AS ?source)
                    ?source ?relationship ?target .
                    FILTER(isIRI(?target) && CONTAINS(STR(?relationship), "urn:relationship:"))
                }} UNION {{
                    BIND(<http://example.org/urn:node:{node_id}> AS ?target)
                    ?source ?relationship ?target .
                    FILTER(isIRI(?source) && CONTAINS(STR(?relationship), "urn:relationship:"))
                }}
                ?source ?source_p ?source_o .
                ?target ?target_p ?target_o .
            }}
        """
        print(query)
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()

        connections_map = {}
        nodes_map = {}

        for result in results["results"]["bindings"]:
            source_uri = result["source"]["value"]
            target_uri = result["target"]["value"]
            relationship_uri = result["relationship"]["value"]

            source_id = source_uri.split("urn:node:")[1]
            target_id = target_uri.split("urn:node:")[1]

            if source_id not in nodes_map:
                nodes_map[source_id] = {}
            if target_id not in nodes_map:
                nodes_map[target_id] = {}

            source_p = result["source_p"]["value"]
            if source_p.startswith("urn:property:"):
                prop_name = source_p.split("urn:property:")[1]
                nodes_map[source_id][prop_name] = result["source_o"]["value"]

            target_p = result["target_p"]["value"]
            if target_p.startswith("urn:property:"):
                prop_name = target_p.split("urn:property:")[1]
                nodes_map[target_id][prop_name] = result["target_o"]["value"]

            connection_key = (source_id, target_id, relationship_uri)
            if connection_key not in connections_map:
                connections_map[connection_key] = relationship_uri.split(
                    "urn:relationship:"
                )[1]

        connections = []
        for (source_id, target_id, _), rel_name in connections_map.items():
            source_node = nodes_map.get(source_id)
            target_node = nodes_map.get(target_id)
            if source_node and target_node:
                connections.append(
                    (source_node, {"relationship": rel_name}, target_node)
                )
        print("connections", connections)
        return connections

    async def extract_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        query = f"""
            SELECT ?p ?o
            WHERE {{
                <http://example.org/urn:node:{to_quoted_uri(node_id)}> ?p ?o .
            }}
        """
        self.sparql.setQuery(query)
        self.sparql.setMethod(POST)
        print(query)
        results = self.sparql.query().convert()
        if not results["results"]["bindings"]:
            return None

        properties = {"id": node_id}
        for result in results["results"]["bindings"]:
            predicate = result["p"]["value"]
            obj = result["o"]["value"]
            if "urn:property:" in predicate:
                prop_name = predicate.split("urn:property:")[1]
                properties[prop_name] = obj
            elif predicate.endswith("1999/02/22-rdf-syntax-ns#type"):
                if "urn:schema:" in obj:
                    properties["type"] = obj.split("urn:schema:")[1]

        return properties

    async def extract_nodes(self, node_ids: List[str]) -> List[Dict[str, Any]]:
        if not node_ids:
            return []

        values_clause = " ".join(
            [
                f"<http://example.org/urn:node:{to_quoted_uri(node_id)}>"
                for node_id in node_ids
            ]
        )
        query = f"""
            SELECT ?node ?p ?o
            WHERE {{
                VALUES ?node {{ {values_clause} }}
                ?node ?p ?o .
            }}
        """
        self.sparql.setQuery(query)
        self.sparql.setMethod(POST)
        results = self.sparql.query().convert()

        nodes = {node_id: {"id": node_id} for node_id in node_ids}
        for result in results["results"]["bindings"]:
            node_uri = result["node"]["value"]
            node_id = node_uri.split("urn:node:")[1]

            predicate = result["p"]["value"]
            obj = result["o"]["value"]

            if "urn:property:" in predicate:
                prop_name = predicate.split("urn:property:")[1]
                nodes[node_id][prop_name] = obj
            elif predicate.endswith("1999/02/22-rdf-syntax-ns#type"):
                if "urn:schema:" in obj:
                    nodes[node_id]["type"] = obj.split("urn:schema:")[1]

        return [properties for properties in nodes.values() if len(properties) > 1]
