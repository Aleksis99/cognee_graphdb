import pytest
from unittest.mock import patch, MagicMock
from cognee.infrastructure.databases.graph import get_graph_engine

@pytest.mark.asyncio
async def test_ontotext_graph_completion():
    with patch("SPARQLWrapper.SPARQLWrapper") as mock_sparql_wrapper:
        # Mock the SPARQLWrapper object
        mock_sparql = MagicMock()
        mock_sparql_wrapper.return_value = mock_sparql

        # Configure the graph engine to use Ontotext GraphDB
        # This requires setting the environment variables for the Ontotext GraphDB connection
        # For example:
        # export GRAPH_DATABASE_PROVIDER=ontotext
        # export GRAPH_DATABASE_ENDPOINT=http://localhost:7200
        # export GRAPH_DATABASE_REPOSITORY=test

        graph_engine = await get_graph_engine()

        # Check if the graph engine is an instance of OntotextGraphDB
        from cognee.infrastructure.databases.graph.ontotext.get_ontotext_graph_db import OntotextGraphDB
        assert isinstance(graph_engine, OntotextGraphDB)

        # Add some nodes and edges
        await graph_engine.add_node("1", {"name": "Alice"})
        await graph_engine.add_node("2", {"name": "Bob"})
        await graph_engine.add_edge("1", "2", "knows")

        # Mock the return value of the query method
        mock_sparql.query.return_value.convert.return_value = {
            "results": {
                "bindings": [
                    {
                        "s": {"value": "urn:node:1"},
                        "p": {"value": "urn:property:name"},
                        "o": {"value": "Alice"},
                    },
                    {
                        "s": {"value": "urn:node:2"},
                        "p": {"value": "urn:property:name"},
                        "o": {"value": "Bob"},
                    },
                    {
                        "s": {"value": "urn:node:1"},
                        "p": {"value": "urn:relationship:knows"},
                        "o": {"value": "urn:node:2"},
                    },
                ]
            }
        }

        # Get the graph data
        nodes, edges = await graph_engine.get_graph_data()

        # Check if the nodes and edges are correct
        assert len(nodes) == 2
        assert len(edges) == 1

        # Clean up the graph
        await graph_engine.delete_graph()
