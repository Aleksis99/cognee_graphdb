from typing import Optional, Dict, Any, List, Tuple, Type, Union
from uuid import UUID
from SPARQLWrapper import SPARQLWrapper, JSON
from cognee.infrastructure.databases.graph.graph_db_interface import GraphDBInterface, NodeData, Node, EdgeData
from cognee.infrastructure.engine import DataPoint

class OntotextGraphDB(GraphDBInterface):
    def __init__(self, endpoint: str, repository: str, username: Optional[str] = None, password: Optional[str] = None):
        self.sparql = SPARQLWrapper(f"{endpoint}/repositories/{repository}")
        if username and password:
            self.sparql.setCredentials(username, password)
        self.sparql.setReturnFormat(JSON)

    async def query(self, query: str, params: dict) -> List[Any]:
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        return results["results"]["bindings"]

    def _construct_insert_query(self, triples: List[Tuple[str, str, str]]) -> str:
        query = "INSERT DATA { "
        for subject, predicate, obj in triples:
            if predicate == "a":
                query += f"<{subject}> a <{obj}> . "
            elif obj.startswith("urn:"):
                query += f"<{subject}> <{predicate}> <{obj}> . "
            else:
                query += f"<{subject}> <{predicate}> \"{obj}\" . "
        query += "}"
        return query

    async def add_node(
        self, node_id: str, properties: Optional[Dict[str, Any]] = None
    ) -> None:
        triples = [(f"urn:node:{node_id}", "a", "urn:schema:node")]
        if properties:
            for key, value in properties.items():
                triples.append((f"urn:node:{node_id}", f"urn:property:{key}", str(value)))
        if triples:
            query = self._construct_insert_query(triples)
            self.sparql.setQuery(query)
            self.sparql.method = 'POST'
            self.sparql.query()

    async def add_nodes(self, nodes: Union[List[Node], List[DataPoint]]) -> None:
        triples = []
        for node in nodes:
            if isinstance(node, DataPoint):
                node_id = str(node.id)
                properties = node.payload
            else:
                node_id, properties = node

            triples.append((f"urn:node:{node_id}", "a", "urn:schema:node"))
            for key, value in properties.items():
                triples.append((f"urn:node:{node_id}", f"urn:property:{key}", str(value)))

        if triples:
            query = self._construct_insert_query(triples)
            self.sparql.setQuery(query)
            self.sparql.method = 'POST'
            self.sparql.query()

    async def delete_node(self, node_id: str) -> None:
        query = f"""
            DELETE WHERE {{
                <urn:node:{node_id}> ?p ?o .
            }}
        """
        self.sparql.setQuery(query)
        self.sparql.method = 'POST'
        self.sparql.query()

    async def delete_nodes(self, node_ids: List[str]) -> None:
        for node_id in node_ids:
            await self.delete_node(node_id)

    async def get_node(self, node_id: str) -> Optional[NodeData]:
        query = f"""
            SELECT ?p ?o
            WHERE {{
                <urn:node:{node_id}> ?p ?o .
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

        return properties

    async def get_nodes(self, node_ids: List[str]) -> List[NodeData]:
        nodes = []
        for node_id in node_ids:
            node = await self.get_node(node_id)
            if node:
                nodes.append(node)
        return nodes

    async def add_edge(
        self,
        source_id: str,
        target_id: str,
        relationship_name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        triples = [(f"urn:node:{source_id}", f"urn:relationship:{relationship_name}", f"urn:node:{target_id}")]
        query = self._construct_insert_query(triples)
        self.sparql.setQuery(query)
        self.sparql.method = 'POST'
        self.sparql.query()

    async def add_edges(
        self, edges: Union[List[EdgeData], List[Tuple[str, str, str, Optional[Dict[str, Any]]]]]
    ) -> None:
        triples = []
        for edge in edges:
            source_id, target_id, relationship_name, _ = edge
            triples.append((f"urn:node:{source_id}", f"urn:relationship:{relationship_name}", f"urn:node:{target_id}"))

        if triples:
            query = self._construct_insert_query(triples)
            self.sparql.setQuery(query)
            self.sparql.method = 'POST'
            self.sparql.query()

    async def delete_graph(self) -> None:
        query = "DELETE WHERE { ?s ?p ?o }"
        self.sparql.setQuery(query)
        self.sparql.method = 'POST'
        self.sparql.query()

    async def get_graph_data(self) -> Tuple[List[Node], List[EdgeData]]:
        query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()

        nodes = {}
        edges = []

        for result in results["results"]["bindings"]:
            subject = result["s"]["value"]
            predicate = result["p"]["value"]
            obj = result["o"]["value"]

            if subject.startswith("urn:node:"):
                node_id = subject.split("urn:node:")[1]
                if node_id not in nodes:
                    nodes[node_id] = {}

                if predicate.startswith("urn:property:"):
                    prop_name = predicate.split("urn:property:")[1]
                    nodes[node_id][prop_name] = obj
                elif predicate.startswith("urn:relationship:"):
                    rel_name = predicate.split("urn:relationship:")[1]
                    if obj.startswith("urn:node:"):
                        target_id = obj.split("urn:node:")[1]
                        edges.append((node_id, target_id, rel_name, {}))

        node_list = [(node_id, properties) for node_id, properties in nodes.items()]

        return node_list, edges

    async def get_graph_metrics(self, include_optional: bool = False) -> Dict[str, Any]:
        node_count_query = "SELECT (COUNT(DISTINCT ?s) as ?node_count) WHERE { ?s a <urn:schema:node> . }"
        self.sparql.setQuery(node_count_query)
        results = self.sparql.query().convert()
        node_count = int(results["results"]["bindings"][0]["node_count"]["value"])

        edge_count_query = "SELECT (COUNT(?s) as ?edge_count) WHERE { ?s ?p ?o . FILTER(STRSTARTS(STR(?p), \"urn:relationship:\")) }"
        self.sparql.setQuery(edge_count_query)
        results = self.sparql.query().convert()
        edge_count = int(results["results"]["bindings"][0]["edge_count"]["value"])

        return {
            "node_count": node_count,
            "edge_count": edge_count,
        }

    async def has_edge(self, source_id: str, target_id: str, relationship_name: str) -> bool:
        query = f"""
            ASK WHERE {{
                <urn:node:{source_id}> <urn:relationship:{relationship_name}> <urn:node:{target_id}> .
            }}
        """
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()
        return results["boolean"]

    async def has_edges(self, edges: List[EdgeData]) -> List[EdgeData]:
        existing_edges = []
        for edge in edges:
            source_id, target_id, relationship_name, _ = edge
            if await self.has_edge(source_id, target_id, relationship_name):
                existing_edges.append(edge)
        return existing_edges

    async def get_edges(self, node_id: str) -> List[EdgeData]:
        query = f"""
            SELECT ?p ?o
            WHERE {{
                <urn:node:{node_id}> ?p ?o .
                FILTER(STRSTARTS(STR(?p), \"urn:relationship:\"))
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
            SELECT DISTINCT ?neighbor
            WHERE {{
                {{
                    <urn:node:{node_id}> ?p ?neighbor .
                    FILTER(isIRI(?neighbor))
                }}
                UNION
                {{
                    ?neighbor ?p <urn:node:{node_id}> .
                    FILTER(isIRI(?neighbor))
                }}
            }}
        """
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()

        neighbors = []
        for result in results["results"]["bindings"]:
            neighbor_uri = result["neighbor"]["value"]
            neighbor_id = neighbor_uri.split("urn:node:")[1]
            neighbor_node = await self.get_node(neighbor_id)
            if neighbor_node:
                neighbors.append(neighbor_node)

        return neighbors

    async def get_nodeset_subgraph(
        self, node_type: Type[Any], node_name: List[str]
    ) -> Tuple[List[Tuple[int, dict]], List[Tuple[int, int, str, dict]]]:
        values_clause = " ".join([f"<urn:node:{node_id}>" for node_id in node_name])
        query = f"""
            CONSTRUCT {{ ?s ?p ?o }}
            WHERE {{
                ?s ?p ?o .
                VALUES ?s {{ {values_clause} }}
            }}
        """
        self.sparql.setQuery(query)
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
        connections = []

        # Outgoing connections
        query = f"""
            SELECT ?p ?o
            WHERE {{
                <urn:node:{node_id}> ?p ?o .
                FILTER(isIRI(?o))
            }}
        """
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()

        for result in results["results"]["bindings"]:
            predicate = result["p"]["value"].split("urn:relationship:")[1]
            target_uri = result["o"]["value"]
            target_id = target_uri.split("urn:node:")[1]
            source_node = await self.get_node(str(node_id))
            target_node = await self.get_node(target_id)
            if source_node and target_node:
                connections.append((source_node, {"relationship": predicate}, target_node))

        # Incoming connections
        query = f"""
            SELECT ?s ?p
            WHERE {{
                ?s ?p <urn:node:{node_id}> .
                FILTER(isIRI(?s))
            }}
        """
        self.sparql.setQuery(query)
        results = self.sparql.query().convert()

        for result in results["results"]["bindings"]:
            predicate = result["p"]["value"].split("urn:relationship:")[1]
            source_uri = result["s"]["value"]
            source_id = source_uri.split("urn:node:")[1]
            source_node = await self.get_node(source_id)
            target_node = await self.get_node(str(node_id))
            if source_node and target_node:
                connections.append((source_node, {"relationship": predicate}, target_node))

        return connections
