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
        

    def find_user_with_ratings(self, user_id):
        query = """
        MATCH (u:User {userId: $user_id})-[r:RATED]->(m:Movie)
        RETURN u, r, m
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id)
            return [(record['u'], record['r'], record['m']) for record in result]

# Ejemplo de uso
if __name__ == "__main__":
    uri = "neo4j+s://8fbf1ea2.databases.neo4j.io"  # Ajustar según la configuración de Neo4j
    user = "neo4j"
    password = "qNmBcaoDG7appXRWYEfnwAA05VyrDXedf0QEQmCUd4I"
    
    graph = GraphDB(uri, user, password)
    
    # Crear nodos generales
    users = [
        {"userId": 1, "name": "Alice"},
        {"userId": 2, "name": "Bob"},
        {"userId": 3, "name": "Charlie"},
        {"userId": 4, "name": "David"},
        {"userId": 5, "name": "Eve"}
    ]
    movies = [
        {"movieId": 101, "title": "Inception", "year": 2010, "plot": "A mind-bending thriller"},
        {"movieId": 102, "title": "The Matrix", "year": 1999, "plot": "A hacker discovers reality is an illusion"},
        {"movieId": 103, "title": "Interstellar", "year": 2014, "plot": "A journey through space and time"}
    ]
    persons = [
        {"name": "Leonardo DiCaprio", "tmdbId": 1, "role": "Actor"},
        {"name": "Christopher Nolan", "tmdbId": 2, "role": "Director"},
        {"name": "Keanu Reeves", "tmdbId": 3, "role": "Actor"}
    ]
    genres = [
        {"name": "Sci-Fi"},
        {"name": "Thriller"}
    ]

    for user in users:
        graph.create_node("User", user)
    for movie in movies:
        graph.create_node("Movie", movie)
    for person in persons:
        graph.create_node("Person", person)
    for genre in genres:
        graph.create_node("Genre", genre)

    # Crear relaciones
    ratings = [
        ("User", "userId", 1, "Movie", "movieId", 101, "RATED", {"rating": 5, "timestamp": 1610000000}),
        ("User", "userId", 2, "Movie", "movieId", 102, "RATED", {"rating": 4, "timestamp": 1610005000}),
        ("User", "userId", 3, "Movie", "movieId", 103, "RATED", {"rating": 5, "timestamp": 1610025000})
    ]
    for relationship in ratings:
        graph.create_relationship(*relationship)

    graph.create_relationship("Person", "name", "Leonardo DiCaprio", "Movie", "movieId", 101, "ACTED_IN", {"role": "Dominick Cobb"})
    graph.create_relationship("Person", "name", "Christopher Nolan", "Movie", "movieId", 101, "DIRECTED", {})
    graph.create_relationship("Person", "name", "Keanu Reeves", "Movie", "movieId", 102, "ACTED_IN", {"role": "Neo"})
    graph.create_relationship("Movie", "movieId", 101, "Genre", "name", "Sci-Fi", "IN_GENRE", {})
    graph.create_relationship("Movie", "movieId", 102, "Genre", "name", "Sci-Fi", "IN_GENRE", {})

    graph.close()
