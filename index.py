import lucene, os, json, shutil
from lucene import *
from org.apache.lucene.store import SimpleFSDirectory, NIOFSDirectory
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions
from org.apache.lucene.analysis.standard import StandardAnalyzer, StandardTokenizer
from org.apache.lucene.document import Document, Field, StringField, TextField, IntPoint, StoredField, FloatPoint, FieldType
from org.apache.lucene.util import Version
from java.nio.file import Files, Paths
from org.apache.lucene.analysis.core import LowerCaseFilter
from org.apache.lucene.analysis.ngram import EdgeNGramTokenFilter, EdgeNGramTokenizer
from org.apache.lucene.search import IndexSearcher, BoostQuery, Query
from org.apache.lucene.analysis import Analyzer
import time
import matplotlib.pyplot as plt

lucene.initVM()

class AutocompleteAnalyzer(Analyzer):
    def __init__(self, side, minlength, maxlength):
        lucene.PythonAnalyzer.__init__(self)
        self.side = side
        self.minlength = minlength
        self.maxlength = maxlength
        
    def createComponents(self, fieldName):
        tokenizer = StandardTokenizer()
        filter = LowerCaseFilter(tokenizer)
        filter = EdgeNGramTokenFilter(filter, 1, 20)
        return Analyzer.TokenStreamComponents(tokenizer, filter)

autocomplete = FieldType()
autocomplete.setTokenized(True)
autocomplete.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
autocomplete.setStored(True)
autocomplete.setStoreTermVectors(True)

keyword = FieldType()
keyword.setTokenized(False)
keyword.setIndexOptions(IndexOptions.DOCS)
keyword.setStored(True)
keyword.setStoreTermVectors(True)

geo_point = FieldType()
geo_point.setTokenized(False)
geo_point.setStored(True)

def map_price_to_numeric(price_symbol):
    price_mapping = {'$': 1, '$$': 2, '$$$': 3, '$$$$': 4}
    return price_mapping.get(price_symbol, 0)

def index_document(writer, fsq_id, name, categories, chains, geocodes, location, rating, price_symbol):
    doc = Document()
    doc.add(Field("fsq_id", fsq_id, keyword))
    doc.add(Field("name", name, keyword))
    
    category_names = " ".join([cat.get("name", "") for cat in categories])
    if(len(category_names) > 0):
        doc.add(Field("categories", category_names[0], keyword))
    
    chain_names = " ".join([chain.get("name", "") for chain in chains])
    if(len(chain_names) > 0):
        doc.add(Field("chains", chain_names, keyword))

            
    try:
        geocodes_main_value = f'{geocodes["latitude"]},{geocodes["longitude"]}'
    except KeyError:
        geocodes_main_value = ""
    doc.add(Field("geocodes_main", geocodes_main_value, geo_point))

    locality_value = location.get("locality", "")
    doc.add(Field("locality", locality_value, keyword))

    region_value = location.get("region", "")
    doc.add(Field("region", region_value, keyword))

    postcode_value = location.get("postcode", "")
    doc.add(Field("postcode", postcode_value, keyword))

    country_value = location.get("country", "")
    doc.add(Field("country", country_value, keyword))

    formatted_address_value = location.get("formatted_address", "")
    doc.add(StoredField("formatted_address", formatted_address_value))
    
    doc.add(FloatPoint("rating", float(rating)))
    doc.add(StoredField("rating", float(rating)))
    
    numeric_price = map_price_to_numeric(price_symbol)
    doc.add(IntPoint("price", int(numeric_price)))
    doc.add(StoredField("price", int(numeric_price)))
    
    writer.addDocument(doc)

def index_documents_from_directory(writer, directory_path):
    for root, dirs, files in os.walk(directory_path):
        for d in dirs:
            for root_sub, dirs_sub, files_sub in os.walk(os.path.join(root, d)):
                for file_name in files_sub:
                    if file_name.endswith('.json'):
                        file_path = os.path.join(root_sub, file_name)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as file:
                                json_data = json.load(file)
                                for result in json_data.get('results', []):
                                    fsq_id = result['fsq_id']
                                    name = result['name']
                                    categories = result['categories']
                                    chains = result['chains']
                                    geocodes = result['geocodes']['main']
                                    location = result['location']
                                    rating = result['rating']
                                    price_symbol = result['price']
                                    index_document(writer, fsq_id, name, categories, chains, geocodes, location, rating, price_symbol)
                                    y.append(time.time() - st)
                        except Exception as e:
                            print(f"Error processing file: {file_path}")
                            print(f"Error details: {str(e)}")
                                    

def main(data_directory):
    index_documents_from_directory(writer, data_directory)


if __name__ == "__main__":
    # data_directory = '/home/cs242/trial_data'
    #For Data 
    data_directory = '/home/cs242/data_f1/Data'
    data_directory1 = '/home/cs242/data_f1/Data2'
    #index = 0

    y = []
    st = time.time()

    index_dir_path = os.path.expanduser('~/LuceneIndexAutocomplete1')
    if os.path.exists(index_dir_path):
        try:
            for filename in os.listdir(index_dir_path):
                file_path = os.path.join(index_dir_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f"Failed to delete {file_path}. Reason: {e}")
    
            print(f"Contents of directory {index_dir_path} deleted.")
        
        except OSError as e:
            print(f"Error: {e.strerror}")
    else:
        try:
            os.makedirs(index_dir_path)
            print(f"Directory {index_dir_path} created.")
        except OSError as e:
            print(f"Error: {e.strerror}")
    
    analyzer = StandardAnalyzer()      
    config = IndexWriterConfig(analyzer)
    store = SimpleFSDirectory(Paths.get(index_dir_path))
    writer = IndexWriter(store, config) 
    
    main(data_directory)
    main(data_directory1)
    writer.commit()
    writer.close()





