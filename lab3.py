from neo4j import GraphDatabase

class GraphDB:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def create_node(self, label, properties):
        query = f"CREATE (n:{label} {{ {', '.join([f'{k}: ${k}' for k in properties.keys()])} }})"
        with self.driver.session() as session:
            session.run(query, **properties)
    
    def create_relationship(self, label1, property1, label2, property2, rel_type, properties={}):
        query = f"""
        MATCH (a:{label1} {{ {property1[0]}: ${property1[0]} }}), 
              (b:{label2} {{ {property2[0]}: ${property2[0]} }})
        CREATE (a)-[r:{rel_type} {{ {', '.join([f'{k}: ${k}' for k in properties.keys()])} }}]->(b)
        """
        with self.driver.session() as session:
            session.run(query, **{property1[0]: property1[1], property2[0]: property2[1]}, **properties)
    
    def find_node(self, label, property_key, property_value):
        query = f"MATCH (n:{label} {{ {property_key}: ${property_key} }}) RETURN n"
        with self.driver.session() as session:
            result = session.run(query, **{property_key: property_value})
            return [record['n'] for record in result]
    
    def find_relationship(self, label1, property1, rel_type, label2, property2):
        query = f"""
        MATCH (a:{label1} {{ {property1[0]}: ${property1[0]} }})-[r:{rel_type}]->(b:{label2} {{ {property2[0]}: ${property2[0]} }})
        RETURN a, r, b
        """
        with self.driver.session() as session:
            result = session.run(query, **{property1[0]: property1[1], property2[0]: property2[1]})
            return [(record['a'], record['r'], record['b']) for record in result]

# Ejemplo de uso
if __name__ == "__main__":
    uri = "neo4j+s://8fbf1ea2.databases.neo4j.io"  # Ajustar según la configuración de Neo4j
    user = "neo4j"
    password = "qNmBcaoDG7appXRWYEfnwAA05VyrDXedf0QEQmCUd4I"
    
    graph = GraphDB(uri, user, password)
    
    # Crear nodos generales
    graph.create_node("User", {"userId": 1, "name": "Alice"})
    graph.create_node("User", {"userId": 2, "name": "Bob"})
    graph.create_node("User", {"userId": 3, "name": "Charlie"})
    graph.create_node("User", {"userId": 4, "name": "David"})
    graph.create_node("User", {"userId": 5, "name": "Eve"})
    graph.create_node("Movie", {"movieId": 101, "title": "Inception", "year": 2010, "plot": "A mind-bending thriller"})
    graph.create_node("Movie", {"movieId": 102, "title": "The Matrix", "year": 1999, "plot": "A hacker discovers reality is an illusion"})
    graph.create_node("Movie", {"movieId": 103, "title": "Interstellar", "year": 2014, "plot": "A journey through space and time"})
    graph.create_node("Movie", {"movieId": 104, "title": "The Dark Knight", "year": 2008, "plot": "Batman faces off against the Joker"})
    graph.create_node("Movie", {"movieId": 105, "title": "Avatar", "year": 2009, "plot": "A paraplegic soldier is sent to Pandora"})

    # Crear relaciones generales
    graph.create_relationship("User", ("userId", 1), "Movie", ("movieId", 101), "RATED", {"rating": 5, "timestamp": 1610000000})
    graph.create_relationship("User", ("userId", 1), "Movie", ("movieId", 102), "RATED", {"rating": 4, "timestamp": 1610005000})
    graph.create_relationship("User", ("userId", 2), "Movie", ("movieId", 101), "RATED", {"rating": 3, "timestamp": 1610010000})
    graph.create_relationship("User", ("userId", 2), "Movie", ("movieId", 104), "RATED", {"rating": 5, "timestamp": 1610015000})
    graph.create_relationship("User", ("userId", 3), "Movie", ("movieId", 103), "RATED", {"rating": 4, "timestamp": 1610020000})
    graph.create_relationship("User", ("userId", 3), "Movie", ("movieId", 105), "RATED", {"rating": 5, "timestamp": 1610025000})
    graph.create_relationship("User", ("userId", 4), "Movie", ("movieId", 101), "RATED", {"rating": 4, "timestamp": 1610030000})
    graph.create_relationship("User", ("userId", 4), "Movie", ("movieId", 102), "RATED", {"rating": 5, "timestamp": 1610035000})
    graph.create_relationship("User", ("userId", 5), "Movie", ("movieId", 104), "RATED", {"rating": 3, "timestamp": 1610040000})
    graph.create_relationship("User", ("userId", 5), "Movie", ("movieId", 105), "RATED", {"rating": 4, "timestamp": 1610045000})

    # Consultar nodos y relaciones
    print(graph.find_node("User", "userId", 1))
    print(graph.find_node("Movie", "movieId", 101))
    print(graph.find_relationship("User", ("userId", 1), "RATED", "Movie", ("movieId", 101)))
    
    graph.close()
