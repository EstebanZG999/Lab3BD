from neo4j import GraphDatabase

# Configuración de conexión
URI = "neo4j+s://7ec489de.databases.neo4j.io"  # Cambia a tu URI de Neo4j si es diferente
USER = "neo4j"               
PASSWORD = "O4q-vqmHZ5DfrCJXHGRXzjPTq0l4FrtTPJu5cYPqOsY"        

# Crear el driver para conectarse a Neo4j
driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def test_connection(tx):
    """Ejecuta una consulta de prueba en Neo4j."""
    return tx.run("MATCH (n) RETURN n LIMIT 5").data()

# Ejecutar consulta de prueba
with driver.session() as session:
    result = session.read_transaction(test_connection)
    print("Resultados:", result)

driver.close()
