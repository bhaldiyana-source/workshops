# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Satellite Imagery Deep Learning
# MAGIC
# MAGIC ## Overview
# MAGIC Learn how to apply deep learning techniques to satellite imagery for object detection, classification, and analysis. This demo covers CNNs for building and road detection, multi-spectral analysis, and practical applications.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Image preprocessing and augmentation for satellite data
# MAGIC - Building detection using Convolutional Neural Networks
# MAGIC - Road network extraction with deep learning
# MAGIC - Multi-spectral and multi-temporal analysis
# MAGIC - Transfer learning from pre-trained models
# MAGIC - Post-processing and vectorization of results
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Understanding of deep learning and CNNs
# MAGIC - Familiarity with PyTorch or TensorFlow
# MAGIC - Basic knowledge of remote sensing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages
%pip install torch torchvision rasterio geopandas numpy pandas matplotlib scikit-image scikit-learn albumentations segmentation-models-pytorch opencv-python-headless pillow --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import torchvision
from torchvision import transforms, models

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image
import cv2

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix

import albumentations as A
from albumentations.pytorch import ToTensorV2

import segmentation_models_pytorch as smp

import warnings
warnings.filterwarnings('ignore')

# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Synthetic Satellite Imagery Generation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Synthetic Satellite Images

# COMMAND ----------

def generate_synthetic_satellite_image(size=256, n_buildings=10, n_roads=5):
    """
    Generate synthetic satellite-like images with buildings and roads
    
    Returns:
        image: RGB image (H, W, 3)
        building_mask: Binary mask for buildings (H, W)
        road_mask: Binary mask for roads (H, W)
    """
    # Create base image (ground/vegetation)
    np.random.seed(None)  # Use random seed for variety
    image = np.random.randint(60, 100, (size, size, 3), dtype=np.uint8)
    
    # Add texture (vegetation patterns)
    noise = np.random.randint(-20, 20, (size, size, 3))
    image = np.clip(image + noise, 0, 255).astype(np.uint8)
    
    # Make it more greenish
    image[:, :, 1] += 40  # Boost green channel
    image = np.clip(image, 0, 255).astype(np.uint8)
    
    # Initialize masks
    building_mask = np.zeros((size, size), dtype=np.uint8)
    road_mask = np.zeros((size, size), dtype=np.uint8)
    
    # Add buildings
    for _ in range(n_buildings):
        # Random building position and size
        x = np.random.randint(20, size - 40)
        y = np.random.randint(20, size - 40)
        w = np.random.randint(15, 35)
        h = np.random.randint(15, 35)
        
        # Draw building on image (grayish/white)
        color = np.random.randint(180, 240)
        cv2.rectangle(image, (x, y), (x+w, y+h), (color, color, color), -1)
        
        # Add some shadows/highlights
        shadow_offset = np.random.randint(2, 5)
        cv2.rectangle(image, (x+shadow_offset, y+shadow_offset), (x+w, y+h), 
                     (color-30, color-30, color-30), -1)
        cv2.rectangle(image, (x, y), (x+w, y+h), (color, color, color), -1)
        
        # Draw on building mask
        cv2.rectangle(building_mask, (x, y), (x+w, y+h), 255, -1)
    
    # Add roads
    for _ in range(n_roads):
        # Random road
        orientation = np.random.choice(['horizontal', 'vertical', 'diagonal'])
        road_width = np.random.randint(8, 15)
        
        if orientation == 'horizontal':
            y_pos = np.random.randint(20, size - 20)
            cv2.rectangle(image, (0, y_pos), (size, y_pos + road_width), 
                         (80, 80, 80), -1)
            cv2.rectangle(road_mask, (0, y_pos), (size, y_pos + road_width), 
                         255, -1)
        elif orientation == 'vertical':
            x_pos = np.random.randint(20, size - 20)
            cv2.rectangle(image, (x_pos, 0), (x_pos + road_width, size), 
                         (80, 80, 80), -1)
            cv2.rectangle(road_mask, (x_pos, 0), (x_pos + road_width, size), 
                         255, -1)
        else:  # diagonal
            pt1 = (np.random.randint(0, size//2), np.random.randint(0, size//2))
            pt2 = (np.random.randint(size//2, size), np.random.randint(size//2, size))
            cv2.line(image, pt1, pt2, (80, 80, 80), road_width)
            cv2.line(road_mask, pt1, pt2, 255, road_width)
    
    return image, building_mask, road_mask

# Generate sample images
sample_images = []
sample_building_masks = []
sample_road_masks = []

for i in range(4):
    img, bldg_mask, road_mask = generate_synthetic_satellite_image(size=256, n_buildings=12, n_roads=4)
    sample_images.append(img)
    sample_building_masks.append(bldg_mask)
    sample_road_masks.append(road_mask)

# Visualize samples
fig, axes = plt.subplots(3, 4, figsize=(15, 10))
for i in range(4):
    axes[0, i].imshow(sample_images[i])
    axes[0, i].set_title(f'Image {i+1}')
    axes[0, i].axis('off')
    
    axes[1, i].imshow(sample_building_masks[i], cmap='gray')
    axes[1, i].set_title(f'Buildings {i+1}')
    axes[1, i].axis('off')
    
    axes[2, i].imshow(sample_road_masks[i], cmap='gray')
    axes[2, i].set_title(f'Roads {i+1}')
    axes[2, i].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/sample_satellite_data.png', dpi=150, bbox_inches='tight')
plt.show()

print("✓ Synthetic satellite imagery generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Data Preparation and Augmentation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Dataset Class

# COMMAND ----------

class SatelliteDataset(Dataset):
    """PyTorch Dataset for satellite imagery"""
    
    def __init__(self, images, masks, transform=None):
        """
        Args:
            images: List of images (numpy arrays)
            masks: List of masks (numpy arrays)
            transform: Albumentations transform
        """
        self.images = images
        self.masks = masks
        self.transform = transform
    
    def __len__(self):
        return len(self.images)
    
    def __getitem__(self, idx):
        image = self.images[idx]
        mask = self.masks[idx]
        
        # Apply transforms
        if self.transform:
            transformed = self.transform(image=image, mask=mask)
            image = transformed['image']
            mask = transformed['mask']
        
        # Convert mask to tensor
        if not isinstance(mask, torch.Tensor):
            mask = torch.from_numpy(mask).long()
        
        # Normalize mask to [0, 1]
        mask = (mask > 0).long()
        
        return image, mask

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Augmentation Pipeline

# COMMAND ----------

# Training transforms (with augmentation)
train_transform = A.Compose([
    A.Resize(256, 256),
    A.HorizontalFlip(p=0.5),
    A.VerticalFlip(p=0.5),
    A.RandomRotate90(p=0.5),
    A.ShiftScaleRotate(shift_limit=0.0625, scale_limit=0.1, rotate_limit=45, p=0.5),
    A.OneOf([
        A.GaussNoise(var_limit=(10.0, 50.0), p=1.0),
        A.GaussianBlur(blur_limit=3, p=1.0),
    ], p=0.3),
    A.OneOf([
        A.RandomBrightnessContrast(brightness_limit=0.2, contrast_limit=0.2, p=1.0),
        A.HueSaturationValue(hue_shift_limit=20, sat_shift_limit=30, val_shift_limit=20, p=1.0),
    ], p=0.3),
    A.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225)),
    ToTensorV2()
])

# Validation transforms (no augmentation)
val_transform = A.Compose([
    A.Resize(256, 256),
    A.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225)),
    ToTensorV2()
])

print("✓ Augmentation pipeline defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Training Dataset

# COMMAND ----------

# Generate larger dataset
print("Generating training dataset...")
n_samples = 200

all_images = []
all_masks = []

for i in range(n_samples):
    img, bldg_mask, road_mask = generate_synthetic_satellite_image(
        size=256, 
        n_buildings=np.random.randint(5, 15),
        n_roads=np.random.randint(3, 6)
    )
    all_images.append(img)
    all_masks.append(bldg_mask)  # Focus on building detection
    
    if (i + 1) % 50 == 0:
        print(f"  Generated {i + 1}/{n_samples} samples")

print(f"✓ Generated {len(all_images)} training samples")

# Split into train/val
train_images, val_images, train_masks, val_masks = train_test_split(
    all_images, all_masks, test_size=0.2, random_state=42
)

print(f"  Training samples: {len(train_images)}")
print(f"  Validation samples: {len(val_images)}")

# Create datasets
train_dataset = SatelliteDataset(train_images, train_masks, transform=train_transform)
val_dataset = SatelliteDataset(val_images, val_masks, transform=val_transform)

# Create dataloaders
batch_size = 8
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True, num_workers=0)
val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False, num_workers=0)

print(f"✓ DataLoaders created (batch_size={batch_size})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Building Detection with U-Net

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define U-Net Model

# COMMAND ----------

# Use segmentation_models_pytorch for U-Net
model = smp.Unet(
    encoder_name="resnet34",        # Encoder backbone
    encoder_weights="imagenet",     # Pre-trained weights
    in_channels=3,                  # RGB input
    classes=2,                      # Binary segmentation (background + buildings)
    activation=None                 # We'll use softmax in loss
)

model = model.to(device)

print("Model architecture:")
print(f"  Encoder: ResNet34 (pre-trained on ImageNet)")
print(f"  Decoder: U-Net")
print(f"  Input: 3 channels (RGB)")
print(f"  Output: 2 classes (background, buildings)")
print(f"  Parameters: {sum(p.numel() for p in model.parameters()):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training Configuration

# COMMAND ----------

# Loss function
criterion = nn.CrossEntropyLoss()

# Optimizer
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Learning rate scheduler
scheduler = optim.lr_scheduler.ReduceLROnPlateau(
    optimizer, mode='min', factor=0.5, patience=3, verbose=True
)

# Metrics
def calculate_iou(pred, target, n_classes=2):
    """Calculate IoU for each class"""
    ious = []
    pred = pred.view(-1)
    target = target.view(-1)
    
    for cls in range(n_classes):
        pred_inds = pred == cls
        target_inds = target == cls
        intersection = (pred_inds & target_inds).sum().float()
        union = (pred_inds | target_inds).sum().float()
        
        if union == 0:
            ious.append(float('nan'))
        else:
            ious.append((intersection / union).item())
    
    return ious

print("✓ Training configuration set")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training Loop

# COMMAND ----------

def train_epoch(model, dataloader, criterion, optimizer, device):
    """Train for one epoch"""
    model.train()
    
    running_loss = 0.0
    running_iou = 0.0
    n_batches = 0
    
    for images, masks in dataloader:
        images = images.to(device)
        masks = masks.to(device)
        
        # Forward pass
        outputs = model(images)
        loss = criterion(outputs, masks)
        
        # Backward pass
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        
        # Metrics
        preds = torch.argmax(outputs, dim=1)
        iou = calculate_iou(preds, masks)
        
        running_loss += loss.item()
        running_iou += iou[1] if not np.isnan(iou[1]) else 0
        n_batches += 1
    
    epoch_loss = running_loss / n_batches
    epoch_iou = running_iou / n_batches
    
    return epoch_loss, epoch_iou

def validate_epoch(model, dataloader, criterion, device):
    """Validate for one epoch"""
    model.eval()
    
    running_loss = 0.0
    running_iou = 0.0
    n_batches = 0
    
    with torch.no_grad():
        for images, masks in dataloader:
            images = images.to(device)
            masks = masks.to(device)
            
            # Forward pass
            outputs = model(images)
            loss = criterion(outputs, masks)
            
            # Metrics
            preds = torch.argmax(outputs, dim=1)
            iou = calculate_iou(preds, masks)
            
            running_loss += loss.item()
            running_iou += iou[1] if not np.isnan(iou[1]) else 0
            n_batches += 1
    
    epoch_loss = running_loss / n_batches
    epoch_iou = running_iou / n_batches
    
    return epoch_loss, epoch_iou

# Training
n_epochs = 10
train_losses = []
val_losses = []
train_ious = []
val_ious = []

print("Starting training...")
print(f"Epochs: {n_epochs}")
print(f"Batch size: {batch_size}")
print(f"Device: {device}\n")

for epoch in range(n_epochs):
    # Train
    train_loss, train_iou = train_epoch(model, train_loader, criterion, optimizer, device)
    train_losses.append(train_loss)
    train_ious.append(train_iou)
    
    # Validate
    val_loss, val_iou = validate_epoch(model, val_loader, criterion, device)
    val_losses.append(val_loss)
    val_ious.append(val_iou)
    
    # Learning rate scheduling
    scheduler.step(val_loss)
    
    print(f"Epoch {epoch+1}/{n_epochs}")
    print(f"  Train Loss: {train_loss:.4f}, Train IoU: {train_iou:.4f}")
    print(f"  Val Loss: {val_loss:.4f}, Val IoU: {val_iou:.4f}")
    print()

print("✓ Training complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot Training History

# COMMAND ----------

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))

# Loss plot
ax1.plot(train_losses, label='Train Loss', marker='o')
ax1.plot(val_losses, label='Val Loss', marker='s')
ax1.set_xlabel('Epoch')
ax1.set_ylabel('Loss')
ax1.set_title('Training and Validation Loss')
ax1.legend()
ax1.grid(True)

# IoU plot
ax2.plot(train_ious, label='Train IoU', marker='o')
ax2.plot(val_ious, label='Val IoU', marker='s')
ax2.set_xlabel('Epoch')
ax2.set_ylabel('IoU')
ax2.set_title('Training and Validation IoU')
ax2.legend()
ax2.grid(True)

plt.tight_layout()
plt.savefig('/dbfs/training_history.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Inference and Visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Make Predictions

# COMMAND ----------

def predict_image(model, image, transform, device):
    """Make prediction on a single image"""
    model.eval()
    
    # Apply transform
    transformed = transform(image=image, mask=np.zeros_like(image[:, :, 0]))
    image_tensor = transformed['image'].unsqueeze(0).to(device)
    
    # Predict
    with torch.no_grad():
        output = model(image_tensor)
        pred = torch.argmax(output, dim=1).squeeze().cpu().numpy()
    
    return pred

# Get some validation samples
sample_indices = np.random.choice(len(val_images), 6, replace=False)

fig, axes = plt.subplots(6, 3, figsize=(12, 20))

for idx, sample_idx in enumerate(sample_indices):
    image = val_images[sample_idx]
    gt_mask = val_masks[sample_idx]
    
    # Predict
    pred_mask = predict_image(model, image, val_transform, device)
    
    # Calculate IoU
    iou = calculate_iou(
        torch.from_numpy(pred_mask).flatten(),
        torch.from_numpy((gt_mask > 0).astype(np.int64)).flatten()
    )[1]
    
    # Plot
    axes[idx, 0].imshow(image)
    axes[idx, 0].set_title('Input Image')
    axes[idx, 0].axis('off')
    
    axes[idx, 1].imshow(gt_mask, cmap='gray')
    axes[idx, 1].set_title('Ground Truth')
    axes[idx, 1].axis('off')
    
    axes[idx, 2].imshow(pred_mask, cmap='gray')
    axes[idx, 2].set_title(f'Prediction (IoU: {iou:.3f})')
    axes[idx, 2].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/predictions.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overlay Predictions on Images

# COMMAND ----------

def overlay_prediction(image, pred_mask, alpha=0.5):
    """Overlay prediction mask on image"""
    overlay = image.copy()
    
    # Create colored mask (red for buildings)
    colored_mask = np.zeros_like(image)
    colored_mask[pred_mask > 0] = [255, 0, 0]  # Red for buildings
    
    # Blend
    result = cv2.addWeighted(overlay, 1 - alpha, colored_mask, alpha, 0)
    
    return result

# Create overlays
fig, axes = plt.subplots(2, 3, figsize=(15, 10))

for idx in range(6):
    row = idx // 3
    col = idx % 3
    
    if idx < len(sample_indices):
        sample_idx = sample_indices[idx]
        image = val_images[sample_idx]
        
        # Predict
        pred_mask = predict_image(model, image, val_transform, device)
        
        # Create overlay
        overlay = overlay_prediction(image, pred_mask, alpha=0.4)
        
        axes[row, col].imshow(overlay)
        axes[row, col].set_title(f'Sample {idx+1} - Detected Buildings')
        axes[row, col].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/overlays.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Multi-Spectral Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulating Multi-Spectral Data

# COMMAND ----------

def generate_multispectral_image(size=256):
    """
    Generate synthetic multi-spectral satellite image
    
    Returns:
        image: (H, W, 4) array with RGB + NIR bands
        ndvi: Normalized Difference Vegetation Index
    """
    # RGB channels
    image_rgb, _, _ = generate_synthetic_satellite_image(size=size)
    
    # Simulate NIR (Near-Infrared) channel
    # Vegetation has high NIR reflectance
    nir = np.zeros((size, size), dtype=np.uint8)
    
    # Areas with high green values (vegetation) get high NIR
    green_mask = image_rgb[:, :, 1] > 100
    nir[green_mask] = np.random.randint(150, 255, np.sum(green_mask))
    nir[~green_mask] = np.random.randint(30, 80, np.sum(~green_mask))
    
    # Combine into 4-channel image
    image_ms = np.dstack([image_rgb, nir])
    
    # Calculate NDVI: (NIR - Red) / (NIR + Red)
    nir_float = nir.astype(float)
    red_float = image_rgb[:, :, 0].astype(float)
    
    # Avoid division by zero
    denominator = nir_float + red_float
    denominator[denominator == 0] = 1
    
    ndvi = (nir_float - red_float) / denominator
    
    return image_ms, ndvi

# Generate multi-spectral samples
ms_image, ndvi = generate_multispectral_image(size=256)

# Visualize
fig, axes = plt.subplots(2, 3, figsize=(15, 8))

axes[0, 0].imshow(ms_image[:, :, :3])
axes[0, 0].set_title('RGB Composite')
axes[0, 0].axis('off')

axes[0, 1].imshow(ms_image[:, :, 0], cmap='Reds')
axes[0, 1].set_title('Red Band')
axes[0, 1].axis('off')

axes[0, 2].imshow(ms_image[:, :, 1], cmap='Greens')
axes[0, 2].set_title('Green Band')
axes[0, 2].axis('off')

axes[1, 0].imshow(ms_image[:, :, 2], cmap='Blues')
axes[1, 0].set_title('Blue Band')
axes[1, 0].axis('off')

axes[1, 1].imshow(ms_image[:, :, 3], cmap='gray')
axes[1, 1].set_title('NIR Band')
axes[1, 1].axis('off')

axes[1, 2].imshow(ndvi, cmap='RdYlGn', vmin=-1, vmax=1)
axes[1, 2].set_title('NDVI')
axes[1, 2].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/multispectral.png', dpi=150, bbox_inches='tight')
plt.show()

print(f"NDVI range: {ndvi.min():.3f} to {ndvi.max():.3f}")
print(f"Mean NDVI: {ndvi.mean():.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Post-Processing and Vectorization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Raster Predictions to Vector Polygons

# COMMAND ----------

from shapely.geometry import shape
from skimage import measure
import geopandas as gpd

def vectorize_prediction(pred_mask, min_area=100):
    """
    Convert binary prediction mask to vector polygons
    
    Args:
        pred_mask: Binary prediction mask (H, W)
        min_area: Minimum area threshold (pixels)
    
    Returns:
        GeoDataFrame with building polygons
    """
    # Find contours
    contours = measure.find_contours(pred_mask, 0.5)
    
    # Convert to polygons
    polygons = []
    
    for contour in contours:
        # Simplify contour
        if len(contour) >= 4:  # Need at least 3 points for polygon
            # Convert to (lon, lat) format
            coords = [(point[1], point[0]) for point in contour]
            
            # Close the polygon
            if coords[0] != coords[-1]:
                coords.append(coords[0])
            
            # Create polygon
            try:
                poly = Polygon(coords)
                
                # Filter by area
                if poly.area >= min_area:
                    polygons.append({
                        'geometry': poly,
                        'area': poly.area,
                        'perimeter': poly.length
                    })
            except:
                pass
    
    if len(polygons) > 0:
        gdf = gpd.GeoDataFrame(polygons)
        return gdf
    else:
        return gpd.GeoDataFrame()

# Vectorize a prediction
sample_idx = sample_indices[0]
image = val_images[sample_idx]
pred_mask = predict_image(model, image, val_transform, device)

# Vectorize
buildings_gdf = vectorize_prediction(pred_mask, min_area=50)

print(f"Detected {len(buildings_gdf)} building polygons")
print(f"\nBuilding statistics:")
print(buildings_gdf[['area', 'perimeter']].describe())

# Visualize
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

axes[0].imshow(image)
axes[0].set_title('Input Image')
axes[0].axis('off')

axes[1].imshow(pred_mask, cmap='gray')
axes[1].set_title('Prediction Mask')
axes[1].axis('off')

axes[2].imshow(image)
for idx, row in buildings_gdf.iterrows():
    coords = np.array(row['geometry'].exterior.coords)
    axes[2].plot(coords[:, 0], coords[:, 1], 'r-', linewidth=2)
axes[2].set_title(f'Vectorized Buildings ({len(buildings_gdf)})')
axes[2].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/vectorized.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Data Preparation**: Proper preprocessing and augmentation are critical
# MAGIC 2. **Transfer Learning**: Use pre-trained encoders (ImageNet) for better performance
# MAGIC 3. **U-Net Architecture**: Excellent for pixel-wise segmentation tasks
# MAGIC 4. **Multi-Spectral**: Additional bands (NIR) provide valuable information
# MAGIC 5. **Post-Processing**: Vectorization converts predictions to usable geometries
# MAGIC 6. **Evaluation**: IoU is standard metric for segmentation tasks
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Data Augmentation**: Essential for generalizing from limited data
# MAGIC - **Normalization**: Use ImageNet statistics for pre-trained models
# MAGIC - **Learning Rate**: Use schedulers for adaptive learning
# MAGIC - **Validation**: Always validate on held-out data
# MAGIC - **Class Imbalance**: Address with weighted losses or sampling
# MAGIC - **Batch Size**: Balance between memory and training stability
# MAGIC - **Early Stopping**: Prevent overfitting with patience
# MAGIC - **Ensemble**: Combine multiple models for robustness
# MAGIC
# MAGIC ### Model Architecture Choices
# MAGIC
# MAGIC - **U-Net**: Best for segmentation with limited data
# MAGIC - **DeepLabV3+**: Better for large-scale, multi-class tasks
# MAGIC - **FPN**: Good for multi-scale object detection
# MAGIC - **Mask R-CNN**: Instance segmentation (individual objects)
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC
# MAGIC - Use mixed precision training (FP16)
# MAGIC - Implement gradient accumulation for larger effective batch sizes
# MAGIC - Use efficient data loading with prefetching
# MAGIC - Profile and optimize bottlenecks
# MAGIC - Consider model quantization for deployment
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Learn land cover classification in Demo 5
# MAGIC - Explore change detection in Demo 6
# MAGIC - Build complete pipeline in Lab 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise: Train Your Own Model
# MAGIC
# MAGIC **Challenge**: Extend this demo to:
# MAGIC 1. Detect both buildings AND roads (multi-class segmentation)
# MAGIC 2. Implement a different architecture (DeepLabV3+, FPN)
# MAGIC 3. Add evaluation metrics (precision, recall, F1)
# MAGIC 4. Experiment with different augmentations
# MAGIC 5. Implement test-time augmentation (TTA)
# MAGIC 6. Export model for production deployment
# MAGIC
# MAGIC **Bonus**: Use real satellite imagery from Sentinel-2 or Landsat!

# COMMAND ----------

# Your code here
