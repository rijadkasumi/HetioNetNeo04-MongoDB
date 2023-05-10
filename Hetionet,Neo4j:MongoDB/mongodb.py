import csv

try:
    import re
    import pymongo
    from pymongo import MongoClient
    import pandas as pd
    import json
    import os
except Exception as e:
    print("Please make sure all the packages are installed")


class MongoDB():

    def __init__(self):
        print("Connecting to Mongodb Database...")
        self.data_dir = os.path.join(os.getcwd(), 'data')
        self.client = MongoClient("localhost", 27017)
        print("Connected to Mongo Database successfully")

        self.DB = self.client['HetioNet']
        self.collection = self.DB['Data']


    def create_db(self):
        columns = 0
        for _ in self.collection.find().limit(1):
            columns += 1 # increment for each field in the file,
        if columns != 0:  # if 0 the database needs to be created
            print("Mongo database available")
            return
        print("Creating Mongo database")
        diseases = {} # empty dictionary
        data = {    # Entities
            'Anatomy': {},
            'Gene': {},
            'Disease': {},
            'Compound': {}
        }

        with open(os.path.join(self.data_dir, "nodes.tsv"), "r", encoding='utf-8', newline='') as nodes_file:
            reader = csv.DictReader(nodes_file, delimiter=re.compile(r'[\t\-0-9]')) # (r'[\t\-0-9]') matches any of the following characters: tab ("\t"), "-" (hyphen), or any number between 0 and 9. 
            for row in reader:
                data[row['kind']][row['id']] = row['name'] # id and name from each row 0? king as key value 

        for x, y in data['Disease'].items(): # iterate over the items in data and created new dict for each disease
            diseases[x] = {
                'id': x,
                'name': y,
                "treatment": [],
                "palliate": [],
                "gene": [],
                "location": [],
            }
        # Acronmys for the metaedges 
        # Some relationship for the edge C-treats-Disease,Compound-pallilate-Disease ...    
        edges_map = {
            "CtD": ['target', 'source', "Compound", "treatment"],
            "CpD": ['target', 'source', "Compound", "palliate"],
            "DaG": ['source', 'target', "Gene", "gene"],
            "DdG": ['source', 'target', "Gene", "gene"],
            "DlA": ['source', 'target', "Anatomy", "location"],
            "DuG": ['source', 'target', "Gene", "gene"]

            }
        # To open the directory for the edges
        # for for in metaedges
        with open(os.path.join(self.data_dir, "nodes.tsv"), "r", encoding='utf-8', newline='') as nodes_file:
            reader = csv.DictReader(nodes_file, delimiter=re.compile(r'[\t\-0-9]'))
            for row in reader:
                edge = row['metaedge']
                if edge in edges_map.keys():
                    diseases[row[edges_map[edge][0]]][edges_map[edge][3]].append(
                        data[edges_map[edge][2]][row[edges_map[edge][1]]]
                    )
        self.collection.insert([v for _, v in diseases.items()])

    def query_db(self, query):
        line1 = self.collection.find({"id": query})  # search for the id
        columns = 0   # counter
        for _ in line1:   #
            if columns > 0:   
                break 
            columns += 1 # if no match increment the columns

        if columns == 0:
            line2 = self.collection.find({"name": query}) # search name insted if col == 0
        else:
            line1.rewind()  # if no match using id query rewind to line 1 and iterate again
            line2 = line1   # lin2= line1 if the colum is not 0

        columns = 0  # reset columns to 0
        # store the data that was extracted from the documents line2
        id = ""
        name = ""
        treatment = []
        palliate = []
        gene = []
        location = []

        for i in line2:  #store results of the query
            id = i['id']
            name = i['name']
            treatment.extend(i['treatment'])
            palliate.extend(i['palliate'])
            gene.extend(i['gene'])
            location.extend(i['location'])
            columns += 1

        if columns == 0:
            print(f'The input you entered "{query}" is either incorrect or not a disease.')
            return
        #read database, join item after separation
        def join_db(sep, items):
            return sep.join(items)
        def collectitems(items):
            items = [items[i:i + 10] for i in range(0, len(items), 10)] #list items in len of 10
            if not items:
                return "No Items"
          # create the groups of 10 to be comma strings
            commas = map(lambda x: join_db(", ", x) + ',', items) # using map to apply lamba that takes a x and appends the comma resutls
            return join_db("\n\t", commas)[:-1] # take out comma on the last line 

        print(
            f'For the "{query}"these are the following :',
            f'The Id is :\n\t{query}',
            f'The name of the disease is Name:\n\t{name}',
            f'Drugs that Palliate the Disease"{query}":\n\t{collectitems(palliate)}',
            f'Drugs that Treat The Disease "{query}":\n\t{collectitems(treatment)}',
            f'Genes that cause this Disease to occur"{query}":\n\t{collectitems(gene)}',
            f'Where "{query}" Occurs:\n\t{collectitems(location)}',
            sep='\n'
        )





