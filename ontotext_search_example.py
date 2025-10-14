import os
import asyncio
from cognee.infrastructure.databases.graph import get_graph_engine
from cognee.modules.search.graph_search import search_graph

async def main():
    # Set the environment variables to use the Ontotext GraphDB connector
    os.environ["GRAPH_DATABASE_PROVIDER"] = "ontotext"
    os.environ["GRAPH_DATABASE_ENDPOINT"] = "http://localhost:7200"
    os.environ["GRAPH_DATABASE_REPOSITORY"] = "test"

    # Initialize the graph engine
    graph_engine = await get_graph_engine()

    # Add some data to the graph
    await graph_engine.add_node("1", {"name": "Person"})
    await graph_engine.add_node("2", {"name": "City"})
    await graph_engine.add_node("3", {"name": "London"})
    await graph_engine.add_edge("1", "2", "lives in")
    await graph_engine.add_edge("2", "3", "is a")

    # Perform a graph completion search
    results = await search_graph("Person lives in London", graph_engine)

    # Print the results
    print(results)

    # Clean up the graph
    await graph_engine.delete_graph()

if __name__ == "__main__":
    asyncio.run(main())
