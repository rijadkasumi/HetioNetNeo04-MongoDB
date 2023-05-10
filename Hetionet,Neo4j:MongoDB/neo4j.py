from turtle import pd


try:
    import csv
    import py2neo
    from py2neo import Graph
    import pandas as pd
    import json
    import os
except Exception as e:
    print("Please make sure all the packages are installed")
#path to directory called 'data'
DATA_DIR = os.path.join(os.getcwd(), "data") 

node_types = [
    "Compound",
    "Disease",
    "Gene",
    "Anatomy"
    ]

acronyms = {
    "C": "Compound",
    "D": "Disease",
    "G": "Gene",
    "A": "Anatomy",
    "r": "resembles",
    "t": "treats",
    "p": "palliates",
    "u": "upregulates",
    "d": "downregulates",
    "b": "binds",
    "a": "associates",
    "l": "localizes",
    "e": "expresses",
    "r1": "regulates",
    "c": "covaries",
    "i": "interacts"
    }

edge_types = [
    "CrC", 
    "CtD", 
    "CpD", 
    "CuG", 
    "CbG", 
    "CdG", 
    "DrD", 
    "DuG", 
    "DaG", 
    "DdG", 
    "DlA", 
    "AuG", 
    "AeG", 
    "AdG", 
    "Gr>G", 
    "GcG", 
    "GiG"
    ]
class Neo4jDB():
    def __init__(self):
        print("Connecting to Neo4j Database...")
        self.graph = Graph(uri="bolt://localhost:7687", auth=("neo4j", "bigdata123"))
        print("Connected to Neo4j Database successfully")

    def create_db(self):
        query = "CALL db.indexes()" # set query to Cypher
        query = "MATCH (n) RETURN COUNT(n);"
        result = self.graph.run(query).data()
        if result[0]['COUNT(n)'] != 0:
            print("Neo4j Database is available")
            return

        print("Creating Neo4j Database...")
        #looping thru node_type, load using pandas
        for node_type in node_types:
            file_path = os.path.join(DATA_DIR, f"{node_type.lower()}.tsv")
            if os.path.exists('nodes.tsv'):
                df = pd.read_csv('nodes.tsv') # read nodes.tsv and load into the df
                data = df.to_dict('id')  # convert to dictionaries to_dict
                for record in data:
                    label = acronyms.get[record['name']]  # retrive name value from acronyms
                    properties = {}  # new dictionary properties with the key-value-pairs
                    for key, value in record.items():
                        if key != "id":
                            properties[key] = value
                    node = py2neo.Node(label, **properties)
                    self.graph.create(node) # creates the database with the nodes 

        for edge_type in edge_types:
            file_path = os.path.join(DATA_DIR, f"{edge_type.lower()}.tsv")
            if os.path.exists('edges.tsv'):
                df = pd.read_csv('edges.tsv')
                data = df.to_dict('id')
                for record in data:
                    start_label = acronyms.get(record['startName'])
                    end_label = acronyms.get(record['endName'])

                    properties = {}
                    for key, value in record.items():
                        if key not in ["start_id", "startName", "type", "end_id", "endName"]:
                            properties[key] = value
                    query = f"MATCH (a:{start_label} {{id: '{record['start_id']}'}})," + \
                            f"(b:{end_label} {{id: '{record['end_id']}'}}) " + \
                            f"CREATE (a)-[:{record['type']} {json.dumps(properties)}]->(b)"
                    self.graph.run(query)

        print("Neo4j Database created successfully")

    def query_db(self, compound):
        if compound == "": # if compound empty string match the nodes Compound, Gene , Disease 
            query = f"""
                    MATCH (c:Compound)-[:upregulates]->(:Gene)<-[:downregulates]-(d:Disease) 
                    WHERE NOT (c)-[:treats]->(d)
                    MATCH (c:Compound)-[:downregulates]->(:Gene)<-[:upregulates]-(d:Disease)
                    WHERE NOT (c)-[:treats]->(d)
                    RETURN DISTINCT c.name, d.name
                    """
        else:  # query to retrive the dissease that the given with compound  upregulates or downregulates
            query = f""" 
                    MATCH (c:Compound {{name: "{compound}"}})-[:upregulates]->(:Gene)<-[:downregulates]-(d:Disease)
                    WHERE NOT (c)-[:treats]->(d)
                    MATCH (c:Compound {{name: "{compound}"}})-[:downregulates]->(:Gene)<-[:upregulates]-(d:Disease)
                    WHERE NOT (c)-[:treats]->(d)
                    RETURN DISTINCT c.name, d.name
                    """
        results = self.graph.run(query).data()
        if not results:
             print("No Compound-Disease pairs found")
        else:
             print("Compound-Disease pairs:")
        for result in results:
                print(f"\t{result['c.name']}-{result['d.name']}")

