from neo4j import GraphDatabase

# Configuración de conexión
URI = "neo4j+s://8fbf1ea2.databases.neo4j.io"  # Cambia a tu URI de Neo4j si es diferente
USER = "neo4j"               
PASSWORD = "qNmBcaoDG7appXRWYEfnwAA05VyrDXedf0QEQmCUd4I"        

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
