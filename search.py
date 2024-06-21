import lucene, os, json, shutil
import re
import sys
from lucene import *
from java.nio.file import Files, Paths
from org.apache.lucene.search import BooleanQuery, TermQuery, BooleanClause, TermRangeQuery, IndexSearcher, BoostQuery, Query, BoostQuery, Sort, SortField
from org.apache.lucene.store import SimpleFSDirectory, NIOFSDirectory
from org.apache.lucene.index import Term, IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader
from org.apache.lucene.analysis.standard import StandardAnalyzer, StandardTokenizer
from org.apache.lucene.document import Document, Field, StringField, TextField, IntPoint, StoredField, FloatPoint, FieldType
from org.apache.lucene.util import Version
from org.apache.lucene.analysis.core import LowerCaseFilter
from org.apache.lucene.analysis.ngram import EdgeNGramTokenFilter, EdgeNGramTokenizer
from org.apache.lucene.analysis import Analyzer
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.util import BytesRef
from org.apache.lucene.document import IntPoint
from org.apache.lucene.queries.function import FunctionQuery
from org.apache.lucene.queries.function.valuesource import FloatFieldSource

lucene.initVM()


def map_price_to_numeric(price_symbol):
    price_mapping = {'$': 1, '$$': 2, '$$$': 3, '$$$$': 4}
    return price_mapping.get(price_symbol, 0) 

def custom_score_provider(doc):
    # Retrieve the document's rating and price for scoring purposes
    doc_rating = doc.get('rating')  # Assume 'rating' is stored as a string
    doc_price = doc.get('price')  # Assume 'price' is stored as a string
    numeric_rating = float(doc_rating) if doc_rating is not None else 0
    numeric_price = map_price_to_numeric(doc_price)

    # Apply custom scoring logic here
    score = 1.0
    if numeric_rating >= min_rating:
        score *= 2  # Boost score for higher ratings
    if numeric_price <= max_price:
        score *= 1.5  # Boost score for lower prices

    return score

def searching(storedir, query):
    searchDir = NIOFSDirectory(Paths.get(storedir))
    searcher = IndexSearcher(DirectoryReader.open(searchDir))
    fields = ['name', 'fsq_id', 'price', 'address', 'rating', 'geocodes_main','region','postcode','locality']
    boolean_query = BooleanQuery.Builder()
    terms = query.split(",")
    rating = 0
    price = 0
    for term in terms:
        field, value = term.split(":")
        if field == 'rating':
            # Rating should be greater than or equal to the provided value.
            range_query = FloatPoint.newRangeQuery("rating", float(value), 5.0)
            boosted_rating_query = BoostQuery(range_query, 2.0)
            boolean_query.add(boosted_rating_query, BooleanClause.Occur.MUST)
            rating = value
        elif field == 'price':
            # Price should be less than or equal to the provided value.
            # Convert the price symbol to its numeric equivalent for the range query.
            numeric_value = map_price_to_numeric(value)
            lower_bound = 1
            range_query = IntPoint.newRangeQuery("price", 1, numeric_value)
            boosted_price_query = BoostQuery(range_query, 1.5)
            boolean_query.add(boosted_price_query, BooleanClause.Occur.MUST)
            price = value
        else:
            # For other fields, use a term query.
            term_query = TermQuery(Term(field, value))
            boolean_query.add(term_query, BooleanClause.Occur.MUST)

    parsed_query = boolean_query.build()
    
    #function_query = FunctionScoreQuery(parsed_query, DoubleValuesSource.fromFloatFunction(custom_score_provider))
     
    # sort = Sort([
    #         SortField("rating", SortField.Type.FLOAT, True),  # Rating in descending order
    #         SortField("price", SortField.Type.INT, False)  # Price in ascending order
    #     ]
    # )
    
    reader = searcher.getIndexReader()  

    topDocs = searcher.search(parsed_query, 25).scoreDocs
    topkdocs = []

    if not topDocs:
        print("No documents found.")
    else:
        for hit in topDocs:
            doc = searcher.doc(hit.doc)
            price_content = doc.get("price")
            #print(f"Price field content for document {hit.doc}: {price_content}")

            topkdocs.append({
                "score": hit.score,
                "name": doc.get("name"),
                "fsqid": doc.get("fsq_id"),
                "price": doc.get("price"),
                "rating": doc.get("rating"),
                "city": doc.get("locality"),
                "address": doc.get("formatted_address"),
                "region": doc.get("region"),
                "postcode": doc.get("postcode"),
                "country": doc.get("country")
            })

    for d in topkdocs:
        print(d)


def main():
    if len(sys.argv) != 2:
        print("Usage: python vim.py <input>")
        return
    input_value = sys.argv[1]
    data_directory = os.path.expanduser('~/LuceneIndexAutocomplete1')
    searching(data_directory,input_value)

if __name__ == "__main__":
    main()


