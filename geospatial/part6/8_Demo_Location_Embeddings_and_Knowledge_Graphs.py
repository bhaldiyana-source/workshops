# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Location Embeddings and Knowledge Graphs
# MAGIC
# MAGIC ## Overview
# MAGIC Learn how to create location embeddings, build spatial knowledge graphs, implement graph neural networks for spatial networks, and perform spatial reasoning with learned representations.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Learning spatial location embeddings (Spatial2Vec, Geo2Vec)
# MAGIC - Building spatial knowledge graphs
# MAGIC - Graph neural networks for location data
# MAGIC - Spatial relationship reasoning
# MAGIC - Similarity search with location embeddings
# MAGIC - Multi-modal embeddings (text + location)
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Understanding of embeddings and representation learning
# MAGIC - Familiarity with graph neural networks
# MAGIC - Basic knowledge of knowledge graphs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages
%pip install torch torch-geometric networkx geopandas shapely pandas numpy scikit-learn matplotlib sentence-transformers --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GATConv, SAGEConv
from torch_geometric.data import Data

import networkx as nx
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString
import matplotlib.pyplot as plt
from sklearn.manifold import TSNE
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

import warnings
warnings.filterwarnings('ignore')

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Spatial Location Embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating Synthetic Location Data

# COMMAND ----------

def generate_city_pois(n_pois=100, center_lat=37.7749, center_lon=-122.4194):
    """Generate synthetic Points of Interest (POIs)"""
    np.random.seed(42)
    
    poi_types = ['restaurant', 'hotel', 'park', 'shop', 'museum', 'hospital', 'school', 'cafe']
    
    pois = []
    for i in range(n_pois):
        # Generate clustered POIs (some types cluster together)
        poi_type = np.random.choice(poi_types)
        
        # Slight clustering by type
        type_offset = poi_types.index(poi_type) * 0.01
        
        lat = center_lat + np.random.normal(type_offset, 0.02)
        lon = center_lon + np.random.normal(type_offset, 0.02)
        
        pois.append({
            'poi_id': f'POI_{i:03d}',
            'name': f'{poi_type.capitalize()} {i}',
            'type': poi_type,
            'latitude': lat,
            'longitude': lon,
            'rating': np.random.uniform(3.0, 5.0),
            'price_level': np.random.randint(1, 5)
        })
    
    return pd.DataFrame(pois)

pois_df = generate_city_pois(n_pois=150)

print(f"Generated {len(pois_df)} POIs")
print(f"\nPOI type distribution:\n{pois_df['type'].value_counts()}")
print(f"\nSample POIs:")
print(pois_df.head())

# Visualize
fig, ax = plt.subplots(figsize=(10, 10))

for poi_type in pois_df['type'].unique():
    data = pois_df[pois_df['type'] == poi_type]
    ax.scatter(data['longitude'], data['latitude'], label=poi_type, alpha=0.6, s=50)

ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.set_title('POI Distribution by Type')
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('/dbfs/poi_distribution.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spatial2Vec: Location Embeddings

# COMMAND ----------

class Spatial2Vec(nn.Module):
    """
    Spatial2Vec: Learn location embeddings from coordinates
    
    Based on: A Neural Network Architecture for Spatial Embeddings
    """
    
    def __init__(self, embedding_dim=64):
        super(Spatial2Vec, self).__init__()
        
        self.embedding_dim = embedding_dim
        
        # Frequency encodings for latitude and longitude
        self.n_frequencies = 16
        
        # MLP to combine frequency encodings
        input_dim = self.n_frequencies * 4  # 2 coords * 2 (sin+cos) * n_freq
        self.mlp = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Linear(128, embedding_dim),
            nn.Tanh()
        )
    
    def forward(self, lat, lon):
        """
        Args:
            lat: Latitude tensor (B,)
            lon: Longitude tensor (B,)
        
        Returns:
            Location embeddings (B, embedding_dim)
        """
        # Normalize coordinates to [-1, 1]
        lat_norm = lat / 90.0
        lon_norm = lon / 180.0
        
        # Generate frequency encodings
        frequencies = 2 ** torch.arange(self.n_frequencies, dtype=torch.float32, device=lat.device)
        
        # Apply frequencies
        lat_freq = lat_norm.unsqueeze(-1) * frequencies.unsqueeze(0)
        lon_freq = lon_norm.unsqueeze(-1) * frequencies.unsqueeze(0)
        
        # Sin and cos encodings
        lat_sin = torch.sin(np.pi * lat_freq)
        lat_cos = torch.cos(np.pi * lat_freq)
        lon_sin = torch.sin(np.pi * lon_freq)
        lon_cos = torch.cos(np.pi * lon_freq)
        
        # Concatenate all encodings
        features = torch.cat([lat_sin, lat_cos, lon_sin, lon_cos], dim=-1)
        
        # Pass through MLP
        embeddings = self.mlp(features)
        
        return embeddings

# Create model
spatial2vec = Spatial2Vec(embedding_dim=64).to(device)

# Generate embeddings for POIs
lats = torch.FloatTensor(pois_df['latitude'].values).to(device)
lons = torch.FloatTensor(pois_df['longitude'].values).to(device)

with torch.no_grad():
    poi_embeddings = spatial2vec(lats, lons)

print(f"Generated location embeddings: {poi_embeddings.shape}")
print(f"Embedding dimension: {poi_embeddings.shape[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizing Location Embeddings with t-SNE

# COMMAND ----------

# Reduce to 2D using t-SNE
embeddings_np = poi_embeddings.cpu().numpy()
tsne = TSNE(n_components=2, random_state=42, perplexity=30)
embeddings_2d = tsne.fit_transform(embeddings_np)

# Plot
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Original coordinates colored by type
for poi_type in pois_df['type'].unique():
    data = pois_df[pois_df['type'] == poi_type]
    indices = data.index
    ax1.scatter(data['longitude'], data['latitude'], label=poi_type, alpha=0.6, s=50)

ax1.set_xlabel('Longitude')
ax1.set_ylabel('Latitude')
ax1.set_title('Original Geographic Coordinates')
ax1.legend()
ax1.grid(True, alpha=0.3)

# t-SNE of embeddings colored by type
for poi_type in pois_df['type'].unique():
    indices = pois_df[pois_df['type'] == poi_type].index.tolist()
    ax2.scatter(embeddings_2d[indices, 0], embeddings_2d[indices, 1], label=poi_type, alpha=0.6, s=50)

ax2.set_xlabel('t-SNE Dimension 1')
ax2.set_ylabel('t-SNE Dimension 2')
ax2.set_title('t-SNE of Location Embeddings')
ax2.legend()
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('/dbfs/location_embeddings_tsne.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Building a Spatial Knowledge Graph

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating the Knowledge Graph

# COMMAND ----------

def build_spatial_knowledge_graph(pois_df, distance_threshold=0.02):
    """
    Build knowledge graph from POI data
    
    Nodes: POIs
    Edges: Spatial proximity, same type, similar characteristics
    """
    G = nx.Graph()
    
    # Add nodes
    for idx, row in pois_df.iterrows():
        G.add_node(idx, **row.to_dict())
    
    # Add edges based on spatial proximity
    for i in range(len(pois_df)):
        for j in range(i + 1, len(pois_df)):
            poi_i = pois_df.iloc[i]
            poi_j = pois_df.iloc[j]
            
            # Calculate distance
            dist = np.sqrt((poi_i['latitude'] - poi_j['latitude'])**2 + 
                          (poi_i['longitude'] - poi_j['longitude'])**2)
            
            if dist < distance_threshold:
                G.add_edge(i, j, 
                          edge_type='near', 
                          distance=dist,
                          weight=1.0 / (dist + 1e-6))
    
    # Add edges for same type
    for poi_type in pois_df['type'].unique():
        type_indices = pois_df[pois_df['type'] == poi_type].index.tolist()
        
        for i in range(len(type_indices)):
            for j in range(i + 1, len(type_indices)):
                if not G.has_edge(type_indices[i], type_indices[j]):
                    G.add_edge(type_indices[i], type_indices[j], 
                              edge_type='same_type',
                              weight=0.5)
    
    return G

# Build knowledge graph
kg = build_spatial_knowledge_graph(pois_df, distance_threshold=0.015)

print(f"Knowledge Graph Statistics:")
print(f"  Nodes: {kg.number_of_nodes()}")
print(f"  Edges: {kg.number_of_edges()}")
print(f"  Average degree: {sum(dict(kg.degree()).values()) / kg.number_of_nodes():.2f}")

# Analyze edge types
edge_types = [data['edge_type'] for _, _, data in kg.edges(data=True)]
print(f"\nEdge type distribution:")
for edge_type in set(edge_types):
    count = edge_types.count(edge_type)
    print(f"  {edge_type}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizing the Knowledge Graph

# COMMAND ----------

# Visualize a subset of the graph
fig, ax = plt.subplots(figsize=(14, 14))

# Use actual coordinates for positions
pos = {i: (row['longitude'], row['latitude']) for i, row in pois_df.iterrows()}

# Filter to show only a subset for clarity
subgraph_nodes = list(range(min(50, len(pois_df))))
subgraph = kg.subgraph(subgraph_nodes)

# Color nodes by POI type
node_colors = []
color_map = {poi_type: i for i, poi_type in enumerate(pois_df['type'].unique())}
for node in subgraph.nodes():
    node_colors.append(color_map[pois_df.iloc[node]['type']])

# Draw graph
nx.draw_networkx_nodes(subgraph, pos, node_color=node_colors, node_size=300, 
                       cmap='tab10', alpha=0.7, ax=ax)
nx.draw_networkx_edges(subgraph, pos, alpha=0.2, width=0.5, ax=ax)

# Add labels for a few nodes
label_nodes = dict(list(pos.items())[:10])
labels = {i: pois_df.iloc[i]['name'][:15] for i in label_nodes.keys()}
nx.draw_networkx_labels(subgraph, pos, labels, font_size=8, ax=ax)

ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.set_title('Spatial Knowledge Graph (Subset)')

plt.tight_layout()
plt.savefig('/dbfs/knowledge_graph.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Graph Neural Networks for Spatial Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Data for PyTorch Geometric

# COMMAND ----------

def networkx_to_pyg(G, node_features_df):
    """Convert NetworkX graph to PyTorch Geometric Data"""
    
    # Node mapping
    node_mapping = {node: idx for idx, node in enumerate(G.nodes())}
    
    # Edge list
    edge_index = []
    edge_attr = []
    
    for u, v, data in G.edges(data=True):
        edge_index.append([node_mapping[u], node_mapping[v]])
        edge_index.append([node_mapping[v], node_mapping[u]])  # Undirected
        
        edge_weight = data.get('weight', 1.0)
        edge_attr.append([edge_weight])
        edge_attr.append([edge_weight])
    
    edge_index = torch.LongTensor(edge_index).t().contiguous()
    edge_attr = torch.FloatTensor(edge_attr)
    
    # Node features
    # Use: latitude, longitude, rating, price_level, type (one-hot)
    node_features = []
    poi_types = node_features_df['type'].unique()
    type_to_idx = {t: i for i, t in enumerate(poi_types)}
    
    for node in G.nodes():
        row = node_features_df.iloc[node]
        
        # Numerical features
        features = [
            row['latitude'] / 90.0,  # Normalized
            row['longitude'] / 180.0,
            row['rating'] / 5.0,
            row['price_level'] / 4.0
        ]
        
        # One-hot type
        type_onehot = [0.0] * len(poi_types)
        type_onehot[type_to_idx[row['type']]] = 1.0
        
        features.extend(type_onehot)
        node_features.append(features)
    
    x = torch.FloatTensor(node_features)
    
    data = Data(x=x, edge_index=edge_index, edge_attr=edge_attr)
    
    return data, node_mapping

# Convert to PyTorch Geometric format
pyg_data, node_mapping = networkx_to_pyg(kg, pois_df)

print(f"PyTorch Geometric Data:")
print(f"  Number of nodes: {pyg_data.num_nodes}")
print(f"  Number of edges: {pyg_data.num_edges}")
print(f"  Node feature dimension: {pyg_data.num_node_features}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Graph Attention Network (GAT) for Location Embeddings

# COMMAND ----------

class LocationGAT(nn.Module):
    """
    Graph Attention Network for learning location embeddings
    """
    
    def __init__(self, input_dim, hidden_dim, output_dim, num_heads=4):
        super(LocationGAT, self).__init__()
        
        # GAT layers
        self.conv1 = GATConv(input_dim, hidden_dim, heads=num_heads, concat=True)
        self.conv2 = GATConv(hidden_dim * num_heads, hidden_dim, heads=num_heads, concat=True)
        self.conv3 = GATConv(hidden_dim * num_heads, output_dim, heads=1, concat=False)
        
        self.dropout = nn.Dropout(0.3)
    
    def forward(self, x, edge_index):
        """
        Args:
            x: Node features (N, input_dim)
            edge_index: Edge connectivity (2, E)
        
        Returns:
            Node embeddings (N, output_dim)
        """
        # First GAT layer
        x = self.conv1(x, edge_index)
        x = F.elu(x)
        x = self.dropout(x)
        
        # Second GAT layer
        x = self.conv2(x, edge_index)
        x = F.elu(x)
        x = self.dropout(x)
        
        # Output layer
        x = self.conv3(x, edge_index)
        
        return x

# Create model
input_dim = pyg_data.num_node_features
hidden_dim = 32
output_dim = 64

gnn_model = LocationGAT(input_dim, hidden_dim, output_dim).to(device)

print(f"GNN Model:")
print(f"  Input dimension: {input_dim}")
print(f"  Hidden dimension: {hidden_dim}")
print(f"  Output dimension: {output_dim}")
print(f"  Parameters: {sum(p.numel() for p in gnn_model.parameters()):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training the GNN with Link Prediction

# COMMAND ----------

def train_link_prediction(model, data, epochs=100, lr=0.01):
    """
    Train GNN using link prediction task
    
    Goal: Predict whether an edge exists between two nodes
    """
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    
    # Move data to device
    data = data.to(device)
    
    # Split edges for training
    num_edges = data.edge_index.size(1)
    perm = torch.randperm(num_edges)
    train_size = int(0.8 * num_edges)
    
    train_edge_index = data.edge_index[:, perm[:train_size]]
    test_edge_index = data.edge_index[:, perm[train_size:]]
    
    # Generate negative samples
    def negative_sampling(edge_index, num_nodes, num_neg_samples):
        neg_edges = []
        while len(neg_edges) < num_neg_samples:
            i = torch.randint(0, num_nodes, (1,)).item()
            j = torch.randint(0, num_nodes, (1,)).item()
            if i != j and not ((edge_index[0] == i) & (edge_index[1] == j)).any():
                neg_edges.append([i, j])
        return torch.LongTensor(neg_edges).t().to(device)
    
    history = {'train_loss': [], 'train_acc': []}
    
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        
        # Forward pass
        embeddings = model(data.x, train_edge_index)
        
        # Positive samples
        pos_edges = train_edge_index
        pos_scores = (embeddings[pos_edges[0]] * embeddings[pos_edges[1]]).sum(dim=1)
        pos_scores = torch.sigmoid(pos_scores)
        
        # Negative samples
        neg_edges = negative_sampling(train_edge_index, data.num_nodes, pos_edges.size(1))
        neg_scores = (embeddings[neg_edges[0]] * embeddings[neg_edges[1]]).sum(dim=1)
        neg_scores = torch.sigmoid(neg_scores)
        
        # Loss
        pos_loss = -torch.log(pos_scores + 1e-10).mean()
        neg_loss = -torch.log(1 - neg_scores + 1e-10).mean()
        loss = pos_loss + neg_loss
        
        # Backward
        loss.backward()
        optimizer.step()
        
        # Accuracy
        pos_correct = (pos_scores > 0.5).float().mean()
        neg_correct = (neg_scores <= 0.5).float().mean()
        accuracy = (pos_correct + neg_correct) / 2
        
        history['train_loss'].append(loss.item())
        history['train_acc'].append(accuracy.item())
        
        if (epoch + 1) % 20 == 0:
            print(f"Epoch {epoch+1}/{epochs} - Loss: {loss.item():.4f}, Acc: {accuracy.item():.4f}")
    
    return history

# Train model
print("Training GNN for location embeddings...\n")
history = train_link_prediction(gnn_model, pyg_data, epochs=100, lr=0.01)

# Plot training history
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

ax1.plot(history['train_loss'])
ax1.set_xlabel('Epoch')
ax1.set_ylabel('Loss')
ax1.set_title('Training Loss')
ax1.grid(True)

ax2.plot(history['train_acc'])
ax2.set_xlabel('Epoch')
ax2.set_ylabel('Accuracy')
ax2.set_title('Training Accuracy')
ax2.grid(True)

plt.tight_layout()
plt.savefig('/dbfs/gnn_training.png', dpi=150, bbox_inches='tight')
plt.show()

print("\n✓ GNN training complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Applications of Location Embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Similarity Search

# COMMAND ----------

# Generate final embeddings
gnn_model.eval()
with torch.no_grad():
    pyg_data = pyg_data.to(device)
    location_embeddings = gnn_model(pyg_data.x, pyg_data.edge_index)

location_embeddings_np = location_embeddings.cpu().numpy()

def find_similar_locations(query_idx, embeddings, pois_df, top_k=5):
    """Find most similar locations using embedding cosine similarity"""
    query_emb = embeddings[query_idx:query_idx+1]
    similarities = cosine_similarity(query_emb, embeddings)[0]
    
    # Get top-k
    top_indices = np.argsort(similarities)[-top_k:][::-1]
    
    results = []
    for idx in top_indices:
        results.append({
            'poi_id': pois_df.iloc[idx]['poi_id'],
            'name': pois_df.iloc[idx]['name'],
            'type': pois_df.iloc[idx]['type'],
            'similarity': similarities[idx]
        })
    
    return results

# Test similarity search
query_idx = 10
query_poi = pois_df.iloc[query_idx]

print(f"Query POI:")
print(f"  Name: {query_poi['name']}")
print(f"  Type: {query_poi['type']}")
print(f"  Location: ({query_poi['latitude']:.4f}, {query_poi['longitude']:.4f})")
print(f"\nMost similar locations:")

similar_pois = find_similar_locations(query_idx, location_embeddings_np, pois_df, top_k=6)
for i, poi in enumerate(similar_pois[1:], 1):  # Skip first (itself)
    print(f"  {i}. {poi['name']} ({poi['type']}) - Similarity: {poi['similarity']:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clustering Locations

# COMMAND ----------

from sklearn.cluster import KMeans

# Cluster locations based on embeddings
n_clusters = 5
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
clusters = kmeans.fit_predict(location_embeddings_np)

pois_df['cluster'] = clusters

print(f"Location Clustering Results:\n")
for cluster_id in range(n_clusters):
    cluster_pois = pois_df[pois_df['cluster'] == cluster_id]
    print(f"Cluster {cluster_id}:")
    print(f"  Size: {len(cluster_pois)} POIs")
    print(f"  Types: {cluster_pois['type'].value_counts().to_dict()}")
    print()

# Visualize clusters
fig, ax = plt.subplots(figsize=(12, 10))

for cluster_id in range(n_clusters):
    cluster_data = pois_df[pois_df['cluster'] == cluster_id]
    ax.scatter(cluster_data['longitude'], cluster_data['latitude'], 
              label=f'Cluster {cluster_id}', alpha=0.6, s=100)

ax.set_xlabel('Longitude')
ax.set_ylabel('Latitude')
ax.set_title('Location Clusters from GNN Embeddings')
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('/dbfs/location_clusters.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Multi-Modal Embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combining Text and Location Embeddings

# COMMAND ----------

# Load text embedding model
text_model = SentenceTransformer('all-MiniLM-L6-v2')

# Create text descriptions for POIs
pois_df['description'] = pois_df.apply(
    lambda row: f"A {row['type']} called {row['name']} with {row['rating']:.1f} stars",
    axis=1
)

# Generate text embeddings
text_embeddings = text_model.encode(pois_df['description'].tolist())

print(f"Text embeddings shape: {text_embeddings.shape}")
print(f"Location embeddings shape: {location_embeddings_np.shape}")

# Combine embeddings (simple concatenation)
multimodal_embeddings = np.concatenate([location_embeddings_np, text_embeddings], axis=1)

print(f"Multi-modal embeddings shape: {multimodal_embeddings.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multi-Modal Similarity Search

# COMMAND ----------

def multimodal_search(query_text, query_location, text_embeddings, location_embeddings, 
                     pois_df, alpha=0.5, top_k=5):
    """
    Search using both text and location
    
    Args:
        query_text: Text query
        query_location: (lat, lon) tuple
        alpha: Weight for location vs text (0=only text, 1=only location)
    """
    # Encode text query
    query_text_emb = text_model.encode([query_text])
    
    # Encode location query
    query_lat = torch.FloatTensor([query_location[0]]).to(device)
    query_lon = torch.FloatTensor([query_location[1]]).to(device)
    with torch.no_grad():
        query_loc_emb = spatial2vec(query_lat, query_lon).cpu().numpy()
    
    # Calculate similarities
    text_similarities = cosine_similarity(query_text_emb, text_embeddings)[0]
    loc_similarities = cosine_similarity(query_loc_emb, location_embeddings)[0]
    
    # Combine with weights
    combined_similarities = alpha * loc_similarities + (1 - alpha) * text_similarities
    
    # Get top-k
    top_indices = np.argsort(combined_similarities)[-top_k:][::-1]
    
    results = []
    for idx in top_indices:
        results.append({
            'name': pois_df.iloc[idx]['name'],
            'type': pois_df.iloc[idx]['type'],
            'description': pois_df.iloc[idx]['description'],
            'combined_score': combined_similarities[idx],
            'text_score': text_similarities[idx],
            'location_score': loc_similarities[idx]
        })
    
    return results

# Test multi-modal search
query_text = "nice restaurant with good ratings"
query_location = (37.78, -122.42)  # Somewhere in SF

print(f"Multi-Modal Search:")
print(f"  Text query: '{query_text}'")
print(f"  Location: ({query_location[0]}, {query_location[1]})")
print(f"\nResults (balanced weighting):")

results = multimodal_search(query_text, query_location, text_embeddings, 
                           location_embeddings_np, pois_df, alpha=0.5, top_k=5)

for i, result in enumerate(results, 1):
    print(f"\n{i}. {result['name']}")
    print(f"   Type: {result['type']}")
    print(f"   Combined: {result['combined_score']:.3f} (Text: {result['text_score']:.3f}, Location: {result['location_score']:.3f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Location Embeddings**: Learn dense representations of geographic locations
# MAGIC 2. **Knowledge Graphs**: Model spatial relationships explicitly
# MAGIC 3. **GNNs**: Leverage graph structure for better embeddings
# MAGIC 4. **Multi-Modal**: Combine location with text/images for richer representations
# MAGIC 5. **Applications**: Similarity search, clustering, recommendation
# MAGIC 6. **Spatial Reasoning**: Embeddings enable analogies and reasoning
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Feature Engineering**: Include relevant node features (type, ratings, etc.)
# MAGIC - **Edge Types**: Use multiple edge types for different relationships
# MAGIC - **Normalization**: Normalize coordinates before embedding
# MAGIC - **Negative Sampling**: Important for link prediction training
# MAGIC - **Evaluation**: Use downstream tasks to evaluate embedding quality
# MAGIC - **Dimensionality**: Balance between expressiveness and efficiency
# MAGIC - **Update Strategy**: Periodically retrain as graph evolves
# MAGIC
# MAGIC ### Applications
# MAGIC
# MAGIC - **Recommendation**: "Users who liked this location also liked..."
# MAGIC - **Search**: Semantic location search with natural language
# MAGIC - **Urban Planning**: Identify similar neighborhoods
# MAGIC - **Real Estate**: Find comparable properties
# MAGIC - **Tourism**: Personalized destination recommendations
# MAGIC - **Logistics**: Optimize routing with learned patterns
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Learn generative AI for geospatial in Demo 9
# MAGIC - Apply embeddings in smart city digital twin (Lab 14)
# MAGIC - Build recommendation systems with location embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC **Challenge**: Extend the location embedding system to:
# MAGIC 1. Add temporal dynamics (time-varying embeddings)
# MAGIC 2. Implement heterogeneous graph with multiple node/edge types
# MAGIC 3. Add hierarchical structure (city → neighborhood → POI)
# MAGIC 4. Implement knowledge graph completion (predict missing relationships)
# MAGIC 5. Create location analogies (A is to B as C is to ?)
# MAGIC 6. Build a location-based recommendation system
# MAGIC 7. Visualize attention weights in GAT
# MAGIC
# MAGIC **Bonus**: Incorporate real-world knowledge from OpenStreetMap!

# COMMAND ----------

# Your code here
