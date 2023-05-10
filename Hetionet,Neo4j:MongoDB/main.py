from neo4j import Neo4jDB
from mongodb import MongoDB


if __name__ == "__main__":

    dbMongo = MongoDB()
    dbMongo.create_db() #create mongodb database
    dbNeo4j = Neo4jDB() 
    dbNeo4j.create_db() #create neo4j database
    print("HetioNet Databases are created")
    # query MongoD
    query = input("Please enter a disease name: ")
    dbMongo.query_db(query)
    # query neo4j
    neo4j_query = input("Please enter a compound name: ")
    dbNeo4j.query_db(neo4j_query)
