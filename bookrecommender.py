from py2neo import Graph

def delete_all(graph):
    graph.run("MATCH (n) DETACH DELETE n")
    graph.run("CALL gds.graph.drop('book-ratings')")

def load_data(graph):
    # Load CSV data into Neo4j
    graph.run("""
        LOAD CSV WITH HEADERS FROM 'file:///subset_books.csv' AS row
        CREATE (b:Book {
          id: row.id,
          book_id: row.book_id,
          best_book_id: row.best_book_id,
          work_id: row.work_id,
          books_count: row.books_count,
          isbn: row.isbn,
          isbn13: row.isbn13,
          authors: row.authors,
          original_publication_year: row.original_publication_year,
          original_title: row.original_title,
          title: row.title,
          language_code: row.language_code,
          average_rating: toFloat(row.average_rating),
          ratings_count: toInteger(row.ratings_count),
          work_ratings_count: toInteger(row.work_ratings_count),
          work_text_reviews_count: toInteger(row.work_text_reviews_count),
          ratings_1: toInteger(row.ratings_1),
          ratings_2: toInteger(row.ratings_2),
          ratings_3: toInteger(row.ratings_3),
          ratings_4: toInteger(row.ratings_4),
          ratings_5: toInteger(row.ratings_5),
          image_url: row.image_url,
          small_image_url: row.small_image_url
        })
    """)

    graph.run("""
        LOAD CSV WITH HEADERS FROM 'file:///subset_ratings.csv' AS row
        MERGE (u:User {id: row.user_id})
    """)

    graph.run("""
        LOAD CSV WITH HEADERS FROM 'file:///subset_ratings.csv' AS row
        MATCH (u:User {id: row.user_id}), (b:Book {id: row.book_id})
        MERGE (u)-[r:RATED]->(b)
        ON CREATE SET r.rating = toInteger(row.rating)
    """)

def compute_recommendations(graph):
    # Create graph projection
    graph.run("""
        CALL gds.graph.project(
          'book-ratings',
          ['User','Book'],
          {
            RATED: {
              orientation: 'UNDIRECTED',
              properties: 'rating'
            }
          }
        )
    """)

    # Perform FastRP algorithm for node embeddings
    graph.run("""
        CALL gds.fastRP.mutate('book-ratings',
          {
            embeddingDimension: 5,
            randomSeed: 42,
            mutateProperty: 'embedding',
            relationshipWeightProperty: 'rating',
            iterationWeights: [0.5, 0.8, 1, 1]
          }
        )
        YIELD nodePropertiesWritten
    """)

    # Compute K-Nearest Neighbors
    graph.run("""
        CALL gds.knn.write('book-ratings', {
            topK: 2,
            nodeProperties: ['embedding'],
            randomSeed: 42,
            concurrency: 1,
            sampleRate: 1.0,
            deltaThreshold: 0.0,
            writeRelationshipType: "SIMILAR",
            writeProperty: "score"
        })
        YIELD nodesCompared, relationshipsWritten, similarityDistribution
        RETURN nodesCompared, relationshipsWritten, similarityDistribution.mean as meanSimilarity
    """)

def retrieve_recommendations(graph, user_id):
    # Retrieve recommendations for the user
     
    query = "MATCH (u1:User {id: '"+user_id+"'})-->(b1:Book)\n"\
    +"WITH collect(b1) as products, u1\n"\
    +"MATCH (u1)-[:SIMILAR]->(u2:User)-->(b2:Book)\n"\
    +"WHERE u2 <> u1 and NOT b2 IN products\n"\
    +"RETURN DISTINCT b2.title as bookRecommendation\n"
    
    result = graph.run(query)

    # Return book recommendations
    return [record["bookRecommendation"] for record in result]

# Main program
if __name__ == "__main__":
    # Connect to Neo4j database
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "aniolantonio"))

    # Delete all nodes and relationships
    print("deleting the graph...")
    delete_all(graph)

    # Load data into Neo4j
    print("loading data into Neo4j...")
    load_data(graph)

    # Compute recommendations
    print("Computing graph embedings and similarities via kNN...")
    compute_recommendations(graph)

    while True:
        # Prompt the user to enter a user ID
        user_id = input("Enter user ID for recommendation: ")

        # Retrieve book recommendations for the user
        recommendations = retrieve_recommendations(graph, user_id)

        # Print the recommendations
        print("Book recommendations for User", user_id)
        if recommendations:
            for book in recommendations:
                print(book)
        else:
            print("No recommendations found for the user.")
