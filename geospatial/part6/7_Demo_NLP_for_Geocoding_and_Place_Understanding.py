# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: NLP for Geocoding and Place Understanding
# MAGIC
# MAGIC ## Overview
# MAGIC Learn how to apply Natural Language Processing techniques to geospatial problems including transformer-based geocoding, address parsing, place name disambiguation, and spatial relationship extraction from text.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Building geocoding systems with BERT and transformers
# MAGIC - Address parsing and component extraction
# MAGIC - Place name disambiguation using context
# MAGIC - Fuzzy matching for address normalization
# MAGIC - Spatial relationship extraction from text
# MAGIC - Location entity recognition (NER)
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Understanding of NLP and transformers
# MAGIC - Familiarity with BERT and attention mechanisms
# MAGIC - Basic knowledge of geocoding concepts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages
%pip install transformers torch sentence-transformers fuzzywuzzy python-Levenshtein geopandas shapely pandas numpy scikit-learn matplotlib spacy --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import torch
from transformers import BertTokenizer, BertModel, BertForTokenClassification, AutoTokenizer, AutoModel
from sentence_transformers import SentenceTransformer
from fuzzywuzzy import fuzz, process

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
import re
import json

import warnings
warnings.filterwarnings('ignore')

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Address Component Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating Synthetic Address Data

# COMMAND ----------

# Address components
STREET_NAMES = ['Main', 'Oak', 'Maple', 'Washington', 'Park', 'Broadway', 'First', 'Second', 'Third', 'Market']
STREET_TYPES = ['Street', 'Avenue', 'Boulevard', 'Drive', 'Lane', 'Road', 'Way', 'Court']
CITIES = ['San Francisco', 'New York', 'Los Angeles', 'Chicago', 'Boston', 'Seattle', 'Austin', 'Denver']
STATES = ['CA', 'NY', 'IL', 'TX', 'MA', 'WA', 'CO']
ZIP_CODES = {
    'San Francisco': [94102, 94103, 94104, 94105],
    'New York': [10001, 10002, 10003, 10004],
    'Los Angeles': [90001, 90002, 90003, 90004],
    'Chicago': [60601, 60602, 60603, 60604],
    'Boston': [02101, 02102, 02103, 02104],
    'Seattle': [98101, 98102, 98103, 98104],
    'Austin': [78701, 78702, 78703, 78704],
    'Denver': [80201, 80202, 80203, 80204]
}

# City coordinates (simplified)
CITY_COORDS = {
    'San Francisco': (37.7749, -122.4194),
    'New York': (40.7128, -74.0060),
    'Los Angeles': (34.0522, -118.2437),
    'Chicago': (41.8781, -87.6298),
    'Boston': (42.3601, -71.0589),
    'Seattle': (47.6062, -122.3321),
    'Austin': (30.2672, -97.7431),
    'Denver': (39.7392, -104.9903)
}

def generate_synthetic_address():
    """Generate a synthetic address with components"""
    street_num = np.random.randint(1, 9999)
    street_name = np.random.choice(STREET_NAMES)
    street_type = np.random.choice(STREET_TYPES)
    city = np.random.choice(CITIES)
    state = STATES[CITIES.index(city) % len(STATES)]
    zip_code = np.random.choice(ZIP_CODES[city])
    
    # Add some variations
    variations = [
        # Standard format
        f"{street_num} {street_name} {street_type}, {city}, {state} {zip_code}",
        # Without comma
        f"{street_num} {street_name} {street_type} {city} {state} {zip_code}",
        # Abbreviated street type
        f"{street_num} {street_name} {street_type[:2]}., {city}, {state} {zip_code}",
        # All caps
        f"{street_num} {street_name.upper()} {street_type.upper()}, {city.upper()}, {state} {zip_code}",
    ]
    
    address_str = np.random.choice(variations)
    
    components = {
        'street_number': str(street_num),
        'street_name': street_name,
        'street_type': street_type,
        'city': city,
        'state': state,
        'zip_code': str(zip_code)
    }
    
    # Get coordinates (with some random offset)
    base_lat, base_lon = CITY_COORDS[city]
    lat = base_lat + np.random.normal(0, 0.05)
    lon = base_lon + np.random.normal(0, 0.05)
    
    return address_str, components, lat, lon

# Generate sample addresses
addresses = []
for _ in range(100):
    addr_str, components, lat, lon = generate_synthetic_address()
    addresses.append({
        'address': addr_str,
        **components,
        'latitude': lat,
        'longitude': lon
    })

addresses_df = pd.DataFrame(addresses)

print(f"Generated {len(addresses_df)} synthetic addresses")
print("\nSample addresses:")
for i in range(5):
    print(f"  {addresses_df.iloc[i]['address']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rule-Based Address Parsing

# COMMAND ----------

def parse_address_rules(address_str):
    """
    Parse address using rule-based approach
    
    Returns dictionary of components
    """
    components = {}
    
    # Clean and normalize
    address = address_str.strip()
    
    # Extract ZIP code (5 digits at end)
    zip_match = re.search(r'\b\d{5}\b\s*$', address)
    if zip_match:
        components['zip_code'] = zip_match.group().strip()
        address = address[:zip_match.start()].strip()
    
    # Extract state (2 uppercase letters before ZIP)
    state_match = re.search(r'\b[A-Z]{2}\b\s*$', address)
    if state_match:
        components['state'] = state_match.group().strip()
        address = address[:state_match.start()].strip()
    
    # Split remaining by commas
    parts = [p.strip() for p in address.split(',')]
    
    if len(parts) >= 2:
        # Last part is likely city
        components['city'] = parts[-1]
        
        # First part is street address
        street_parts = parts[0].split()
        if len(street_parts) >= 3:
            components['street_number'] = street_parts[0]
            components['street_type'] = street_parts[-1].replace('.', '')
            components['street_name'] = ' '.join(street_parts[1:-1])
    
    return components

# Test rule-based parsing
print("Rule-Based Address Parsing Examples:\n")
for i in range(5):
    addr = addresses_df.iloc[i]['address']
    parsed = parse_address_rules(addr)
    print(f"Input:  {addr}")
    print(f"Parsed: {parsed}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: BERT-Based Address Component Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Token Classification for Address Parsing

# COMMAND ----------

# Define component labels for token classification
ADDRESS_LABELS = {
    'O': 0,              # Outside/Other
    'B-NUM': 1,          # Begin street number
    'B-STREET': 2,       # Begin street name
    'I-STREET': 3,       # Inside street name
    'B-TYPE': 4,         # Begin street type
    'B-CITY': 5,         # Begin city
    'I-CITY': 6,         # Inside city
    'B-STATE': 7,        # Begin state
    'B-ZIP': 8           # Begin ZIP code
}

ID_TO_LABEL = {v: k for k, v in ADDRESS_LABELS.items()}

print("Address Component Labels:")
for label, idx in sorted(ADDRESS_LABELS.items(), key=lambda x: x[1]):
    print(f"  {idx}: {label}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Training Data for Token Classification

# COMMAND ----------

def tokenize_and_label_address(address_str, components):
    """
    Create token-level labels for address
    
    This is simplified; in production, use proper tokenization
    """
    tokens = address_str.split()
    labels = []
    
    for token in tokens:
        # Remove punctuation for matching
        token_clean = re.sub(r'[^\w]', '', token)
        
        # Assign label based on component
        if token_clean.isdigit() and len(token_clean) <= 5:
            if len(token_clean) == 5:  # Likely ZIP
                labels.append('B-ZIP')
            else:  # Likely street number
                labels.append('B-NUM')
        elif token_clean.upper() in STATES or token_clean in components.get('state', ''):
            labels.append('B-STATE')
        elif token_clean in components.get('city', '').split():
            if len(labels) > 0 and labels[-1] in ['B-CITY', 'I-CITY']:
                labels.append('I-CITY')
            else:
                labels.append('B-CITY')
        elif token_clean in components.get('street_type', '').split():
            labels.append('B-TYPE')
        elif token_clean in components.get('street_name', '').split():
            if len(labels) > 0 and labels[-1] in ['B-STREET', 'I-STREET']:
                labels.append('I-STREET')
            else:
                labels.append('B-STREET')
        else:
            labels.append('O')
    
    return tokens, labels

# Example
sample_addr = addresses_df.iloc[0]['address']
sample_components = addresses_df.iloc[0][['street_number', 'street_name', 'street_type', 'city', 'state', 'zip_code']].to_dict()

tokens, labels = tokenize_and_label_address(sample_addr, sample_components)

print(f"Address: {sample_addr}\n")
print("Tokens and Labels:")
for token, label in zip(tokens, labels):
    print(f"  {token:15s} -> {label}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Fuzzy Matching and Address Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fuzzy String Matching

# COMMAND ----------

def fuzzy_match_address(query, candidates, threshold=80):
    """
    Find best matching address using fuzzy string matching
    
    Args:
        query: Query address string
        candidates: List of candidate addresses
        threshold: Minimum similarity score (0-100)
    
    Returns:
        List of (address, score) tuples
    """
    # Get matches
    matches = process.extract(query, candidates, limit=5, scorer=fuzz.token_sort_ratio)
    
    # Filter by threshold
    matches = [(addr, score) for addr, score in matches if score >= threshold]
    
    return matches

# Create candidate database
candidate_addresses = addresses_df['address'].tolist()

# Test with misspellings and variations
test_queries = [
    "123 Main Stret, San Fransisco, CA 94102",  # Misspelling
    "456 Oak Avenue San Francisco CA",           # Missing punctuation
    "789 BROADWAY BLVD NEW YORK NY",            # All caps, no ZIP
]

print("Fuzzy Address Matching:\n")
for query in test_queries:
    print(f"Query: {query}")
    matches = fuzzy_match_address(query, candidate_addresses, threshold=70)
    print("Top matches:")
    for addr, score in matches[:3]:
        print(f"  [{score}%] {addr}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Place Embeddings with Sentence Transformers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Location Embeddings

# COMMAND ----------

# Load sentence transformer model
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')

# Create place descriptions
place_descriptions = []
for _, row in addresses_df.head(50).iterrows():
    desc = f"{row['street_name']} {row['street_type']} in {row['city']}, {row['state']}"
    place_descriptions.append(desc)

# Generate embeddings
place_embeddings = embedding_model.encode(place_descriptions)

print(f"Generated embeddings for {len(place_embeddings)} places")
print(f"Embedding dimension: {place_embeddings.shape[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semantic Place Search

# COMMAND ----------

def search_places_semantic(query, place_descriptions, embeddings, top_k=5):
    """
    Search for places using semantic similarity
    
    Args:
        query: Natural language query
        place_descriptions: List of place descriptions
        embeddings: Pre-computed embeddings
        top_k: Number of results to return
    
    Returns:
        List of (description, similarity_score) tuples
    """
    # Encode query
    query_embedding = embedding_model.encode([query])
    
    # Calculate similarities
    similarities = cosine_similarity(query_embedding, embeddings)[0]
    
    # Get top-k indices
    top_indices = np.argsort(similarities)[-top_k:][::-1]
    
    results = []
    for idx in top_indices:
        results.append((place_descriptions[idx], similarities[idx]))
    
    return results

# Test semantic search
queries = [
    "street near the park in San Francisco",
    "main road in New York",
    "avenue in California",
]

print("Semantic Place Search:\n")
for query in queries:
    print(f"Query: '{query}'")
    results = search_places_semantic(query, place_descriptions, place_embeddings, top_k=3)
    print("Results:")
    for desc, score in results:
        print(f"  [{score:.3f}] {desc}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Spatial Relationship Extraction

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extracting Spatial Relations from Text

# COMMAND ----------

# Spatial relationship patterns
SPATIAL_PATTERNS = {
    'near': [r'\bnear\b', r'\bclose to\b', r'\badjacent to\b', r'\bnearby\b'],
    'north': [r'\bnorth of\b', r'\bnorthern\b'],
    'south': [r'\bsouth of\b', r'\bsouthern\b'],
    'east': [r'\beast of\b', r'\beastern\b'],
    'west': [r'\bwest of\b', r'\bwestern\b'],
    'inside': [r'\binside\b', r'\bwithin\b', r'\bin\b'],
    'between': [r'\bbetween\b'],
    'across': [r'\bacross from\b', r'\bopposite\b']
}

def extract_spatial_relations(text):
    """
    Extract spatial relationships from text
    
    Returns list of (relation_type, text_span) tuples
    """
    relations = []
    text_lower = text.lower()
    
    for relation_type, patterns in SPATIAL_PATTERNS.items():
        for pattern in patterns:
            matches = re.finditer(pattern, text_lower)
            for match in matches:
                relations.append({
                    'relation': relation_type,
                    'span': match.span(),
                    'text': match.group()
                })
    
    return relations

# Test with example texts
example_texts = [
    "The hospital is north of the park and near the river.",
    "Main Street runs between Oak Avenue and Broadway.",
    "The restaurant is inside the shopping mall, across from the theater.",
    "The hotel is close to the airport in the southern part of the city."
]

print("Spatial Relationship Extraction:\n")
for text in example_texts:
    print(f"Text: {text}")
    relations = extract_spatial_relations(text)
    print("Relations:")
    for rel in relations:
        print(f"  {rel['relation']:10s} -> '{rel['text']}'")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Geocoding with Context

# COMMAND ----------

# MAGIC %md
# MAGIC ### Context-Aware Geocoding

# COMMAND ----------

class ContextualGeocoder:
    """
    Geocoder that uses context to disambiguate place names
    """
    
    def __init__(self, places_df):
        """
        Args:
            places_df: DataFrame with columns: name, city, state, latitude, longitude
        """
        self.places_df = places_df
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Create place descriptions and embeddings
        self.descriptions = []
        for _, row in places_df.iterrows():
            desc = f"{row.get('street_name', '')} {row.get('street_type', '')} in {row.get('city', '')}, {row.get('state', '')}"
            self.descriptions.append(desc)
        
        self.embeddings = self.embedding_model.encode(self.descriptions)
    
    def geocode(self, query, context=None, top_k=3):
        """
        Geocode query with optional context
        
        Args:
            query: Address or place name to geocode
            context: Additional context string
            top_k: Number of candidates to return
        
        Returns:
            List of candidate locations with scores
        """
        # Combine query with context
        if context:
            search_text = f"{query} {context}"
        else:
            search_text = query
        
        # Encode query
        query_emb = self.embedding_model.encode([search_text])
        
        # Calculate similarities
        similarities = cosine_similarity(query_emb, self.embeddings)[0]
        
        # Get top candidates
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        results = []
        for idx in top_indices:
            row = self.places_df.iloc[idx]
            results.append({
                'description': self.descriptions[idx],
                'city': row.get('city'),
                'state': row.get('state'),
                'latitude': row.get('latitude'),
                'longitude': row.get('longitude'),
                'confidence': similarities[idx]
            })
        
        return results

# Create geocoder
geocoder = ContextualGeocoder(addresses_df)

# Test contextual geocoding
queries_with_context = [
    ("Main Street", "looking for restaurants in San Francisco"),
    ("Park Avenue", "hotels near Times Square"),
    ("Broadway", "California"),
]

print("Contextual Geocoding:\n")
for query, context in queries_with_context:
    print(f"Query: '{query}'")
    print(f"Context: '{context}'")
    results = geocoder.geocode(query, context=context, top_k=2)
    print("Results:")
    for res in results:
        print(f"  [{res['confidence']:.3f}] {res['description']}")
        print(f"    Coordinates: ({res['latitude']:.4f}, {res['longitude']:.4f})")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Building a Complete Geocoding Pipeline

# COMMAND ----------

class NLPGeocodingPipeline:
    """
    Complete NLP-based geocoding pipeline
    """
    
    def __init__(self, reference_data):
        """
        Args:
            reference_data: DataFrame with address data
        """
        self.reference_data = reference_data
        self.contextual_geocoder = ContextualGeocoder(reference_data)
        
        # Create lookup for exact matches
        self.address_lookup = {}
        for idx, row in reference_data.iterrows():
            addr_normalized = self._normalize_address(row['address'])
            self.address_lookup[addr_normalized] = {
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'city': row['city'],
                'state': row['state']
            }
    
    def _normalize_address(self, address):
        """Normalize address for matching"""
        # Convert to lowercase, remove punctuation, collapse spaces
        normalized = address.lower()
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized
    
    def geocode(self, query, context=None, method='auto'):
        """
        Geocode an address or place name
        
        Args:
            query: Address/place to geocode
            context: Optional context
            method: 'exact', 'fuzzy', 'semantic', or 'auto'
        
        Returns:
            dict with coordinates and metadata
        """
        # Try exact match first
        query_normalized = self._normalize_address(query)
        
        if method in ['exact', 'auto']:
            if query_normalized in self.address_lookup:
                result = self.address_lookup[query_normalized].copy()
                result['method'] = 'exact_match'
                result['confidence'] = 1.0
                return result
        
        # Try fuzzy matching
        if method in ['fuzzy', 'auto']:
            candidates = list(self.address_lookup.keys())
            fuzzy_matches = fuzzy_match_address(query_normalized, candidates, threshold=85)
            
            if fuzzy_matches:
                best_match = fuzzy_matches[0][0]
                result = self.address_lookup[best_match].copy()
                result['method'] = 'fuzzy_match'
                result['confidence'] = fuzzy_matches[0][1] / 100.0
                return result
        
        # Fall back to semantic matching
        if method in ['semantic', 'auto']:
            semantic_results = self.contextual_geocoder.geocode(query, context=context, top_k=1)
            if semantic_results:
                result = semantic_results[0].copy()
                result['method'] = 'semantic_match'
                return result
        
        return None

# Create pipeline
pipeline = NLPGeocodingPipeline(addresses_df)

# Test pipeline
test_queries = [
    ("456 Oak Avenue, San Francisco, CA 94102", None),  # Exact match (if exists)
    ("123 Main Stret, San Fransisco", None),            # Fuzzy match
    ("restaurant on Broadway in New York", "downtown"), # Semantic with context
]

print("Complete Geocoding Pipeline:\n")
for query, context in test_queries:
    print(f"Query: '{query}'")
    if context:
        print(f"Context: '{context}'")
    
    result = pipeline.geocode(query, context=context)
    
    if result:
        print(f"Method: {result['method']}")
        print(f"Confidence: {result['confidence']:.3f}")
        print(f"Location: ({result['latitude']:.4f}, {result['longitude']:.4f})")
        print(f"City: {result['city']}, State: {result['state']}")
    else:
        print("No match found")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Evaluation and Metrics

# COMMAND ----------

# Evaluate geocoding accuracy
def evaluate_geocoder(pipeline, test_data, distance_threshold=1.0):
    """
    Evaluate geocoder performance
    
    Args:
        pipeline: Geocoding pipeline
        test_data: DataFrame with address and true coordinates
        distance_threshold: Maximum distance in km for correct match
    
    Returns:
        dict with evaluation metrics
    """
    from math import radians, cos, sin, asin, sqrt
    
    def haversine_distance(lat1, lon1, lat2, lon2):
        """Calculate distance between two points in km"""
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        km = 6371 * c
        return km
    
    results = []
    methods_used = []
    
    for _, row in test_data.iterrows():
        query = row['address']
        true_lat = row['latitude']
        true_lon = row['longitude']
        
        result = pipeline.geocode(query)
        
        if result:
            pred_lat = result['latitude']
            pred_lon = result['longitude']
            distance = haversine_distance(true_lat, true_lon, pred_lat, pred_lon)
            
            results.append({
                'correct': distance <= distance_threshold,
                'distance_km': distance,
                'method': result['method'],
                'confidence': result['confidence']
            })
            methods_used.append(result['method'])
    
    # Calculate metrics
    accuracy = sum(1 for r in results if r['correct']) / len(results) if results else 0
    avg_distance = np.mean([r['distance_km'] for r in results]) if results else 0
    avg_confidence = np.mean([r['confidence'] for r in results]) if results else 0
    
    method_counts = pd.Series(methods_used).value_counts()
    
    return {
        'accuracy': accuracy,
        'average_distance_km': avg_distance,
        'average_confidence': avg_confidence,
        'total_queries': len(test_data),
        'successful_geocodes': len(results),
        'methods_used': method_counts.to_dict()
    }

# Evaluate on sample
test_sample = addresses_df.sample(n=min(30, len(addresses_df)), random_state=42)
evaluation = evaluate_geocoder(pipeline, test_sample, distance_threshold=5.0)

print("Geocoding Pipeline Evaluation:")
print("=" * 60)
print(f"Accuracy (within 5km): {evaluation['accuracy']:.1%}")
print(f"Average distance error: {evaluation['average_distance_km']:.2f} km")
print(f"Average confidence: {evaluation['average_confidence']:.3f}")
print(f"Successful geocodes: {evaluation['successful_geocodes']}/{evaluation['total_queries']}")
print(f"\nMethods used:")
for method, count in evaluation['methods_used'].items():
    print(f"  {method}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Hybrid Approach**: Combine rule-based, fuzzy, and semantic methods
# MAGIC 2. **Context Matters**: Use surrounding text to disambiguate locations
# MAGIC 3. **Embeddings**: Sentence transformers enable semantic search
# MAGIC 4. **Fuzzy Matching**: Handle misspellings and variations
# MAGIC 5. **Confidence Scores**: Always provide uncertainty estimates
# MAGIC 6. **Spatial Relations**: Extract relationships from natural language
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Normalization**: Standardize addresses before matching
# MAGIC - **Multi-Stage**: Try exact → fuzzy → semantic in order
# MAGIC - **Confidence Thresholds**: Set appropriate thresholds for each method
# MAGIC - **Context**: Always try to use available context
# MAGIC - **Validation**: Maintain reference database with ground truth
# MAGIC - **Error Handling**: Gracefully handle unparseable addresses
# MAGIC - **Multi-Language**: Use multilingual models when needed
# MAGIC - **Fine-Tuning**: Fine-tune transformers on domain-specific data
# MAGIC
# MAGIC ### Applications
# MAGIC
# MAGIC - **Emergency Response**: Parse addresses from 911 calls
# MAGIC - **Delivery Systems**: Normalize delivery addresses
# MAGIC - **Real Estate**: Match property descriptions to locations
# MAGIC - **Social Media**: Extract location mentions from posts
# MAGIC - **Urban Planning**: Analyze place descriptions in documents
# MAGIC - **Navigation**: Natural language location queries
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Learn location embeddings and knowledge graphs in Demo 8
# MAGIC - Build intelligent geocoding service in Lab 13
# MAGIC - Apply to multi-language scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC **Challenge**: Extend the NLP geocoding system to:
# MAGIC 1. Add support for international addresses
# MAGIC 2. Implement address validation and correction
# MAGIC 3. Extract and use landmark mentions ("near the Eiffel Tower")
# MAGIC 4. Handle abbreviated and informal addresses
# MAGIC 5. Build a REST API for the geocoding service
# MAGIC 6. Add caching for frequently queried addresses
# MAGIC 7. Implement batch geocoding with optimization
# MAGIC
# MAGIC **Bonus**: Fine-tune BERT on real address dataset for better component extraction!

# COMMAND ----------

# Your code here
