# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Change Detection Systems
# MAGIC
# MAGIC ## Overview
# MAGIC Learn advanced change detection techniques for satellite imagery including temporal differencing, deep learning approaches, disaster impact assessment, and urban growth monitoring.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Temporal differencing and image subtraction methods
# MAGIC - Deep learning for change detection (Siamese networks, U-Net++)
# MAGIC - Multi-temporal analysis strategies
# MAGIC - Disaster impact assessment workflows
# MAGIC - Urban growth and deforestation monitoring
# MAGIC - Change magnitude and direction analysis
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Completion of Demos 4-5 (Deep Learning and Land Cover)
# MAGIC - Understanding of temporal analysis concepts
# MAGIC - Familiarity with multi-date satellite imagery

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages
%pip install torch torchvision segmentation-models-pytorch albumentations opencv-python-headless numpy pandas matplotlib scikit-learn scikit-image geopandas shapely pillow --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader

import segmentation_models_pytorch as smp
import albumentations as A
from albumentations.pytorch import ToTensorV2

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import cv2
from PIL import Image
from datetime import datetime, timedelta

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from skimage import morphology

import warnings
warnings.filterwarnings('ignore')

# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# Define change types
CHANGE_CLASSES = {
    0: 'No Change',
    1: 'Urban Expansion',
    2: 'Deforestation',
    3: 'Water Body Change',
    4: 'Agricultural Change'
}

N_CHANGE_CLASSES = len(CHANGE_CLASSES)

# Colors for visualization
CHANGE_COLORS = {
    0: [200, 200, 200],  # No Change - Gray
    1: [255, 0, 0],      # Urban Expansion - Red
    2: [139, 69, 19],    # Deforestation - Brown
    3: [0, 0, 255],      # Water Body Change - Blue
    4: [255, 255, 0]     # Agricultural Change - Yellow
}

print(f"Change Detection Classes: {N_CHANGE_CLASSES}")
for class_id, class_name in CHANGE_CLASSES.items():
    print(f"  {class_id}: {class_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Traditional Change Detection Methods

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1: Image Differencing

# COMMAND ----------

def generate_temporal_pair(size=256, change_type='urban_expansion'):
    """
    Generate before/after image pair with specific change type
    
    Args:
        size: Image size
        change_type: Type of change to simulate
    
    Returns:
        image_t1: Before image
        image_t2: After image
        change_mask: Binary change mask
    """
    np.random.seed(None)
    
    # Create base scene (time 1)
    image_t1 = np.zeros((size, size, 3), dtype=np.uint8)
    image_t1[:] = [34, 139, 34]  # Forest green base
    
    # Add texture
    noise = np.random.randint(-20, 20, (size, size, 3))
    image_t1 = np.clip(image_t1.astype(int) + noise, 0, 255).astype(np.uint8)
    
    # Copy for time 2
    image_t2 = image_t1.copy()
    change_mask = np.zeros((size, size), dtype=np.uint8)
    
    if change_type == 'urban_expansion':
        # Add new buildings in time 2
        n_buildings = np.random.randint(3, 8)
        for _ in range(n_buildings):
            x = np.random.randint(20, size - 40)
            y = np.random.randint(20, size - 40)
            w = np.random.randint(15, 35)
            h = np.random.randint(15, 35)
            
            color = np.random.randint(180, 230)
            cv2.rectangle(image_t2, (x, y), (x+w, y+h), (color, color, color), -1)
            cv2.rectangle(change_mask, (x, y), (x+w, y+h), 1, -1)
    
    elif change_type == 'deforestation':
        # Remove forest patches
        n_patches = np.random.randint(2, 5)
        for _ in range(n_patches):
            x = np.random.randint(20, size - 60)
            y = np.random.randint(20, size - 60)
            w = np.random.randint(30, 60)
            h = np.random.randint(30, 60)
            
            # Change to bare ground
            bare_color = [139, 90, 43]
            cv2.rectangle(image_t2, (x, y), (x+w, y+h), bare_color, -1)
            cv2.rectangle(change_mask, (x, y), (x+w, y+h), 2, -1)
    
    elif change_type == 'water_change':
        # Add/change water bodies
        n_water = np.random.randint(1, 3)
        for _ in range(n_water):
            center_x = np.random.randint(50, size - 50)
            center_y = np.random.randint(50, size - 50)
            radius = np.random.randint(25, 45)
            
            cv2.circle(image_t2, (center_x, center_y), radius, (65, 105, 225), -1)
            cv2.circle(change_mask, (center_x, center_y), radius, 3, -1)
    
    elif change_type == 'agricultural':
        # Add cropland
        n_fields = np.random.randint(2, 4)
        for _ in range(n_fields):
            x = np.random.randint(10, size - 50)
            y = np.random.randint(10, size - 50)
            w = np.random.randint(30, 50)
            h = np.random.randint(30, 50)
            
            crop_color = [np.random.randint(200, 255), np.random.randint(200, 220), np.random.randint(100, 150)]
            cv2.rectangle(image_t2, (x, y), (x+w, y+h), crop_color, -1)
            cv2.rectangle(change_mask, (x, y), (x+w, y+h), 4, -1)
    
    return image_t1, image_t2, change_mask

# Generate sample pairs
fig, axes = plt.subplots(4, 4, figsize=(16, 16))

for i, change_type in enumerate(['urban_expansion', 'deforestation', 'water_change', 'agricultural']):
    img_t1, img_t2, change_mask = generate_temporal_pair(size=256, change_type=change_type)
    
    # Calculate simple difference
    diff = np.abs(img_t2.astype(float) - img_t1.astype(float)).mean(axis=2)
    diff_normalized = (diff / diff.max() * 255).astype(np.uint8)
    
    axes[i, 0].imshow(img_t1)
    axes[i, 0].set_title(f'{change_type.replace("_", " ").title()} - Before')
    axes[i, 0].axis('off')
    
    axes[i, 1].imshow(img_t2)
    axes[i, 1].set_title('After')
    axes[i, 1].axis('off')
    
    axes[i, 2].imshow(diff_normalized, cmap='hot')
    axes[i, 2].set_title('Difference')
    axes[i, 2].axis('off')
    
    axes[i, 3].imshow(change_mask, cmap='gray')
    axes[i, 3].set_title('Ground Truth')
    axes[i, 3].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/change_detection_samples.png', dpi=150, bbox_inches='tight')
plt.show()

print("✓ Temporal pairs generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2: NDVI Differencing

# COMMAND ----------

def calculate_ndvi(image):
    """Calculate NDVI from RGB image (simulated)"""
    # Simulate NIR band from green channel
    red = image[:, :, 0].astype(float)
    green = image[:, :, 1].astype(float)
    nir = green * 1.5  # Simulate NIR
    
    # Calculate NDVI
    denominator = nir + red
    denominator[denominator == 0] = 1
    ndvi = (nir - red) / denominator
    
    return ndvi

# Demonstrate NDVI change detection
img_t1, img_t2, change_mask = generate_temporal_pair(size=256, change_type='deforestation')

ndvi_t1 = calculate_ndvi(img_t1)
ndvi_t2 = calculate_ndvi(img_t2)
ndvi_diff = ndvi_t2 - ndvi_t1

fig, axes = plt.subplots(2, 3, figsize=(15, 10))

axes[0, 0].imshow(img_t1)
axes[0, 0].set_title('Image T1')
axes[0, 0].axis('off')

axes[0, 1].imshow(ndvi_t1, cmap='RdYlGn', vmin=-1, vmax=1)
axes[0, 1].set_title('NDVI T1')
axes[0, 1].axis('off')

axes[0, 2].imshow(img_t2)
axes[0, 2].set_title('Image T2')
axes[0, 2].axis('off')

axes[1, 0].imshow(ndvi_t2, cmap='RdYlGn', vmin=-1, vmax=1)
axes[1, 0].set_title('NDVI T2')
axes[1, 0].axis('off')

axes[1, 1].imshow(ndvi_diff, cmap='RdBu', vmin=-1, vmax=1)
axes[1, 1].set_title('NDVI Difference')
axes[1, 1].axis('off')

axes[1, 2].imshow(change_mask, cmap='gray')
axes[1, 2].set_title('Ground Truth')
axes[1, 2].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/ndvi_change.png', dpi=150, bbox_inches='tight')
plt.show()

print(f"NDVI T1 range: {ndvi_t1.min():.3f} to {ndvi_t1.max():.3f}")
print(f"NDVI T2 range: {ndvi_t2.min():.3f} to {ndvi_t2.max():.3f}")
print(f"NDVI change range: {ndvi_diff.min():.3f} to {ndvi_diff.max():.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Deep Learning Change Detection

# COMMAND ----------

# MAGIC %md
# MAGIC ### Siamese U-Net Architecture

# COMMAND ----------

class SiameseUNet(nn.Module):
    """
    Siamese U-Net for change detection
    
    Processes two images (T1 and T2) through shared encoder,
    then fuses features in decoder for change prediction
    """
    
    def __init__(self, encoder_name="resnet34", encoder_weights="imagenet", n_classes=N_CHANGE_CLASSES):
        super(SiameseUNet, self).__init__()
        
        # Shared encoder (Siamese)
        self.encoder = smp.Unet(
            encoder_name=encoder_name,
            encoder_weights=encoder_weights,
            in_channels=3,
            classes=64,  # Intermediate features
            activation=None
        )
        
        # Change detection head
        self.change_head = nn.Sequential(
            nn.Conv2d(128, 64, kernel_size=3, padding=1),  # 64*2 from concatenation
            nn.ReLU(inplace=True),
            nn.Conv2d(64, 32, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.Conv2d(32, n_classes, kernel_size=1)
        )
    
    def forward(self, x1, x2):
        """
        Args:
            x1: Image at time 1 (B, 3, H, W)
            x2: Image at time 2 (B, 3, H, W)
        
        Returns:
            Change prediction (B, n_classes, H, W)
        """
        # Extract features from both images
        feat1 = self.encoder(x1)
        feat2 = self.encoder(x2)
        
        # Concatenate features
        feat_concat = torch.cat([feat1, feat2], dim=1)
        
        # Predict changes
        change_pred = self.change_head(feat_concat)
        
        return change_pred

# Create model
model = SiameseUNet(encoder_name="resnet34", encoder_weights="imagenet", n_classes=N_CHANGE_CLASSES)
model = model.to(device)

print("Siamese U-Net Model:")
print(f"  Architecture: Siamese U-Net with shared encoder")
print(f"  Encoder: ResNet34 (ImageNet pretrained)")
print(f"  Input: 2 x RGB images (time 1 and time 2)")
print(f"  Output: {N_CHANGE_CLASSES} change classes")
print(f"  Parameters: {sum(p.numel() for p in model.parameters()):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataset Preparation

# COMMAND ----------

class ChangeDetectionDataset(Dataset):
    """PyTorch Dataset for change detection"""
    
    def __init__(self, images_t1, images_t2, masks, transform=None):
        self.images_t1 = images_t1
        self.images_t2 = images_t2
        self.masks = masks
        self.transform = transform
    
    def __len__(self):
        return len(self.images_t1)
    
    def __getitem__(self, idx):
        img_t1 = self.images_t1[idx]
        img_t2 = self.images_t2[idx]
        mask = self.masks[idx]
        
        if self.transform:
            # Apply same transform to both images and mask
            transformed = self.transform(image=img_t1, mask=mask)
            img_t1 = transformed['image']
            mask_transformed = transformed['mask']
            
            # Transform second image with same parameters
            img_t2 = self.transform(image=img_t2, mask=np.zeros_like(mask))['image']
            mask = mask_transformed
        
        if not isinstance(mask, torch.Tensor):
            mask = torch.from_numpy(mask).long()
        
        return img_t1, img_t2, mask

# Augmentation pipelines
train_transform = A.Compose([
    A.Resize(256, 256),
    A.HorizontalFlip(p=0.5),
    A.VerticalFlip(p=0.5),
    A.RandomRotate90(p=0.5),
    A.ShiftScaleRotate(shift_limit=0.0625, scale_limit=0.1, rotate_limit=45, p=0.5),
    A.RandomBrightnessContrast(brightness_limit=0.2, contrast_limit=0.2, p=0.3),
    A.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225)),
    ToTensorV2()
])

val_transform = A.Compose([
    A.Resize(256, 256),
    A.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225)),
    ToTensorV2()
])

# Generate dataset
print("Generating change detection dataset...")
n_samples = 300
change_types = ['urban_expansion', 'deforestation', 'water_change', 'agricultural']

all_images_t1 = []
all_images_t2 = []
all_masks = []

for i in range(n_samples):
    change_type = np.random.choice(change_types)
    img_t1, img_t2, mask = generate_temporal_pair(size=256, change_type=change_type)
    
    all_images_t1.append(img_t1)
    all_images_t2.append(img_t2)
    all_masks.append(mask)
    
    if (i + 1) % 100 == 0:
        print(f"  Generated {i+1}/{n_samples} pairs")

print(f"✓ Generated {len(all_images_t1)} temporal pairs")

# Class distribution
all_masks_flat = np.concatenate([m.flatten() for m in all_masks])
unique, counts = np.unique(all_masks_flat, return_counts=True)
class_distribution = dict(zip(unique, counts))

print(f"\nChange class distribution:")
for class_id in range(N_CHANGE_CLASSES):
    if class_id in class_distribution:
        count = class_distribution[class_id]
        pct = 100 * count / len(all_masks_flat)
        print(f"  {CHANGE_CLASSES[class_id]}: {count:,} ({pct:.2f}%)")

# Split dataset
train_t1, val_t1, train_t2, val_t2, train_masks, val_masks = train_test_split(
    all_images_t1, all_images_t2, all_masks, test_size=0.2, random_state=42
)

print(f"\nTrain samples: {len(train_t1)}")
print(f"Val samples: {len(val_t1)}")

# Create datasets
train_dataset = ChangeDetectionDataset(train_t1, train_t2, train_masks, transform=train_transform)
val_dataset = ChangeDetectionDataset(val_t1, val_t2, val_masks, transform=val_transform)

batch_size = 8
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True, num_workers=0)
val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False, num_workers=0)

print(f"✓ DataLoaders created (batch_size={batch_size})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training

# COMMAND ----------

# Calculate class weights
class_counts = np.array([class_distribution.get(i, 1) for i in range(N_CHANGE_CLASSES)])
class_weights = 1.0 / (class_counts + 1e-6)
class_weights = class_weights / class_weights.sum() * N_CHANGE_CLASSES
class_weights = torch.FloatTensor(class_weights).to(device)

print("Class weights:")
for i, class_name in CHANGE_CLASSES.items():
    print(f"  {class_name}: {class_weights[i]:.4f}")

# Loss and optimizer
criterion = nn.CrossEntropyLoss(weight=class_weights)
optimizer = optim.Adam(model.parameters(), lr=0.001)
scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=3, verbose=True)

def calculate_change_metrics(pred, target, n_classes):
    """Calculate change detection metrics"""
    pred = pred.view(-1).cpu().numpy()
    target = target.view(-1).cpu().numpy()
    
    accuracy = accuracy_score(target, pred)
    
    # IoU per class
    ious = []
    for cls in range(n_classes):
        pred_inds = pred == cls
        target_inds = target == cls
        intersection = np.logical_and(pred_inds, target_inds).sum()
        union = np.logical_or(pred_inds, target_inds).sum()
        
        if union == 0:
            ious.append(float('nan'))
        else:
            ious.append(intersection / union)
    
    valid_ious = [iou for iou in ious if not np.isnan(iou)]
    mean_iou = np.mean(valid_ious) if valid_ious else 0.0
    
    return accuracy, mean_iou

def train_epoch(model, dataloader, criterion, optimizer, device):
    """Train one epoch"""
    model.train()
    running_loss = 0.0
    running_acc = 0.0
    running_iou = 0.0
    n_batches = 0
    
    for img_t1, img_t2, masks in dataloader:
        img_t1 = img_t1.to(device)
        img_t2 = img_t2.to(device)
        masks = masks.to(device)
        
        optimizer.zero_grad()
        outputs = model(img_t1, img_t2)
        loss = criterion(outputs, masks)
        loss.backward()
        optimizer.step()
        
        preds = torch.argmax(outputs, dim=1)
        acc, mean_iou = calculate_change_metrics(preds, masks, N_CHANGE_CLASSES)
        
        running_loss += loss.item()
        running_acc += acc
        running_iou += mean_iou
        n_batches += 1
    
    return running_loss / n_batches, running_acc / n_batches, running_iou / n_batches

def validate_epoch(model, dataloader, criterion, device):
    """Validate one epoch"""
    model.eval()
    running_loss = 0.0
    running_acc = 0.0
    running_iou = 0.0
    n_batches = 0
    
    with torch.no_grad():
        for img_t1, img_t2, masks in dataloader:
            img_t1 = img_t1.to(device)
            img_t2 = img_t2.to(device)
            masks = masks.to(device)
            
            outputs = model(img_t1, img_t2)
            loss = criterion(outputs, masks)
            
            preds = torch.argmax(outputs, dim=1)
            acc, mean_iou = calculate_change_metrics(preds, masks, N_CHANGE_CLASSES)
            
            running_loss += loss.item()
            running_acc += acc
            running_iou += mean_iou
            n_batches += 1
    
    return running_loss / n_batches, running_acc / n_batches, running_iou / n_batches

# Training loop
n_epochs = 12
history = {
    'train_loss': [], 'train_acc': [], 'train_iou': [],
    'val_loss': [], 'val_acc': [], 'val_iou': []
}

print("Starting training...")
print(f"Epochs: {n_epochs}, Batch size: {batch_size}\n")

for epoch in range(n_epochs):
    train_loss, train_acc, train_iou = train_epoch(model, train_loader, criterion, optimizer, device)
    val_loss, val_acc, val_iou = validate_epoch(model, val_loader, criterion, device)
    
    history['train_loss'].append(train_loss)
    history['train_acc'].append(train_acc)
    history['train_iou'].append(train_iou)
    history['val_loss'].append(val_loss)
    history['val_acc'].append(val_acc)
    history['val_iou'].append(val_iou)
    
    scheduler.step(val_loss)
    
    print(f"Epoch {epoch+1}/{n_epochs}")
    print(f"  Train - Loss: {train_loss:.4f}, Acc: {train_acc:.4f}, IoU: {train_iou:.4f}")
    print(f"  Val   - Loss: {val_loss:.4f}, Acc: {val_acc:.4f}, IoU: {val_iou:.4f}")
    print()

print("✓ Training complete!")

# Plot training history
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

axes[0].plot(history['train_loss'], label='Train', marker='o')
axes[0].plot(history['val_loss'], label='Val', marker='s')
axes[0].set_xlabel('Epoch')
axes[0].set_ylabel('Loss')
axes[0].set_title('Loss')
axes[0].legend()
axes[0].grid(True)

axes[1].plot(history['train_acc'], label='Train', marker='o')
axes[1].plot(history['val_acc'], label='Val', marker='s')
axes[1].set_xlabel('Epoch')
axes[1].set_ylabel('Accuracy')
axes[1].set_title('Accuracy')
axes[1].legend()
axes[1].grid(True)

axes[2].plot(history['train_iou'], label='Train', marker='o')
axes[2].plot(history['val_iou'], label='Val', marker='s')
axes[2].set_xlabel('Epoch')
axes[2].set_ylabel('Mean IoU')
axes[2].set_title('Mean IoU')
axes[2].legend()
axes[2].grid(True)

plt.tight_layout()
plt.savefig('/dbfs/change_detection_training.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Inference and Visualization

# COMMAND ----------

def predict_change(model, img_t1, img_t2, transform, device):
    """Predict change between two images"""
    model.eval()
    
    # Transform images
    transformed_t1 = transform(image=img_t1, mask=np.zeros_like(img_t1[:, :, 0]))
    transformed_t2 = transform(image=img_t2, mask=np.zeros_like(img_t2[:, :, 0]))
    
    img_t1_tensor = transformed_t1['image'].unsqueeze(0).to(device)
    img_t2_tensor = transformed_t2['image'].unsqueeze(0).to(device)
    
    with torch.no_grad():
        output = model(img_t1_tensor, img_t2_tensor)
        pred = torch.argmax(output, dim=1).squeeze().cpu().numpy()
    
    return pred

def change_mask_to_color(mask):
    """Convert change mask to RGB"""
    h, w = mask.shape
    color_mask = np.zeros((h, w, 3), dtype=np.uint8)
    
    for class_id, color in CHANGE_COLORS.items():
        color_mask[mask == class_id] = color
    
    return color_mask

# Visualize predictions
sample_indices = np.random.choice(len(val_t1), 6, replace=False)

fig, axes = plt.subplots(6, 4, figsize=(16, 24))

for idx, sample_idx in enumerate(sample_indices):
    img_t1 = val_t1[sample_idx]
    img_t2 = val_t2[sample_idx]
    gt_mask = val_masks[sample_idx]
    
    pred_mask = predict_change(model, img_t1, img_t2, val_transform, device)
    
    # Calculate metrics
    acc, mean_iou = calculate_change_metrics(
        torch.from_numpy(pred_mask).flatten(),
        torch.from_numpy(gt_mask).flatten(),
        N_CHANGE_CLASSES
    )
    
    axes[idx, 0].imshow(img_t1)
    axes[idx, 0].set_title('Before (T1)')
    axes[idx, 0].axis('off')
    
    axes[idx, 1].imshow(img_t2)
    axes[idx, 1].set_title('After (T2)')
    axes[idx, 1].axis('off')
    
    axes[idx, 2].imshow(change_mask_to_color(gt_mask))
    axes[idx, 2].set_title('Ground Truth')
    axes[idx, 2].axis('off')
    
    axes[idx, 3].imshow(change_mask_to_color(pred_mask))
    axes[idx, 3].set_title(f'Prediction\nAcc: {acc:.3f}, IoU: {mean_iou:.3f}')
    axes[idx, 3].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/change_predictions.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Change Magnitude and Direction Analysis

# COMMAND ----------

def analyze_change_magnitude(img_t1, img_t2, change_mask):
    """Analyze magnitude and direction of changes"""
    # Calculate per-pixel change magnitude
    diff = img_t2.astype(float) - img_t1.astype(float)
    magnitude = np.sqrt(np.sum(diff ** 2, axis=2))
    
    # Direction of change (in RGB space)
    # Simplified: use dominant channel change
    channel_change = np.argmax(np.abs(diff), axis=2)
    
    # Statistics per change class
    change_stats = {}
    for class_id, class_name in CHANGE_CLASSES.items():
        if class_id == 0:  # Skip "No Change"
            continue
        
        class_pixels = change_mask == class_id
        if np.sum(class_pixels) > 0:
            change_stats[class_name] = {
                'mean_magnitude': magnitude[class_pixels].mean(),
                'max_magnitude': magnitude[class_pixels].max(),
                'area_pixels': np.sum(class_pixels)
            }
    
    return magnitude, change_stats

# Analyze sample
sample_idx = sample_indices[0]
img_t1 = val_t1[sample_idx]
img_t2 = val_t2[sample_idx]
pred_mask = predict_change(model, img_t1, img_t2, val_transform, device)

magnitude, change_stats = analyze_change_magnitude(img_t1, img_t2, pred_mask)

# Visualize
fig, axes = plt.subplots(1, 4, figsize=(16, 4))

axes[0].imshow(img_t1)
axes[0].set_title('Before')
axes[0].axis('off')

axes[1].imshow(img_t2)
axes[1].set_title('After')
axes[1].axis('off')

axes[2].imshow(change_mask_to_color(pred_mask))
axes[2].set_title('Predicted Changes')
axes[2].axis('off')

im = axes[3].imshow(magnitude, cmap='hot')
axes[3].set_title('Change Magnitude')
axes[3].axis('off')
plt.colorbar(im, ax=axes[3], fraction=0.046)

plt.tight_layout()
plt.savefig('/dbfs/change_magnitude.png', dpi=150, bbox_inches='tight')
plt.show()

# Print statistics
print("Change Statistics:")
print("=" * 60)
for class_name, stats in change_stats.items():
    print(f"\n{class_name}:")
    print(f"  Area: {stats['area_pixels']:,} pixels")
    print(f"  Mean magnitude: {stats['mean_magnitude']:.2f}")
    print(f"  Max magnitude: {stats['max_magnitude']:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Multi-Temporal Analysis

# COMMAND ----------

def generate_time_series(n_timesteps=5, size=256):
    """Generate time series of images showing progressive change"""
    # Start with initial scene
    current_image = np.zeros((size, size, 3), dtype=np.uint8)
    current_image[:] = [34, 139, 34]  # Forest green
    noise = np.random.randint(-20, 20, (size, size, 3))
    current_image = np.clip(current_image.astype(int) + noise, 0, 255).astype(np.uint8)
    
    time_series = [current_image.copy()]
    
    # Gradually add urban development
    urban_zones = []
    for t in range(n_timesteps - 1):
        current_image = time_series[-1].copy()
        
        # Add new buildings
        n_new_buildings = np.random.randint(2, 5)
        for _ in range(n_new_buildings):
            x = np.random.randint(20, size - 40)
            y = np.random.randint(20, size - 40)
            w = np.random.randint(15, 30)
            h = np.random.randint(15, 30)
            
            color = np.random.randint(180, 230)
            cv2.rectangle(current_image, (x, y), (x+w, y+h), (color, color, color), -1)
            urban_zones.append((x, y, w, h))
        
        time_series.append(current_image.copy())
    
    return time_series

# Generate and visualize time series
time_series = generate_time_series(n_timesteps=6, size=256)

fig, axes = plt.subplots(2, 6, figsize=(18, 6))

for t in range(6):
    axes[0, t].imshow(time_series[t])
    axes[0, t].set_title(f'Time {t+1}')
    axes[0, t].axis('off')
    
    if t > 0:
        # Show change from T1
        pred_change = predict_change(model, time_series[0], time_series[t], val_transform, device)
        axes[1, t].imshow(change_mask_to_color(pred_change))
        axes[1, t].set_title(f'Changes from T1')
        axes[1, t].axis('off')
    else:
        axes[1, t].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/time_series.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Siamese Architecture**: Shared weights efficiently process temporal pairs
# MAGIC 2. **Multi-Class Detection**: Identify different types of changes simultaneously
# MAGIC 3. **Traditional Methods**: Image/NDVI differencing still useful for rapid assessment
# MAGIC 4. **Deep Learning**: Superior performance for complex change patterns
# MAGIC 5. **Change Magnitude**: Quantify intensity of changes, not just location
# MAGIC 6. **Time Series**: Multi-temporal analysis reveals progressive changes
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Temporal Alignment**: Ensure precise co-registration of images
# MAGIC - **Seasonal Effects**: Account for phenological changes (not always real change)
# MAGIC - **Atmospheric Correction**: Normalize for atmospheric differences
# MAGIC - **Class Weights**: Handle imbalanced change/no-change ratios
# MAGIC - **Post-Processing**: Remove noise with morphological operations
# MAGIC - **Validation**: Use ground truth from multiple sources
# MAGIC - **Multi-Scale**: Analyze changes at different spatial scales
# MAGIC - **Confidence Scores**: Provide uncertainty estimates
# MAGIC
# MAGIC ### Applications
# MAGIC
# MAGIC - **Disaster Response**: Rapid damage assessment after events
# MAGIC - **Urban Planning**: Monitor urban sprawl and development
# MAGIC - **Environmental**: Track deforestation, desertification
# MAGIC - **Agriculture**: Crop monitoring and yield estimation
# MAGIC - **Infrastructure**: Detect illegal construction or mining
# MAGIC - **Climate**: Monitor glacier retreat, sea level changes
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Learn NLP for geocoding in Demo 7
# MAGIC - Build environmental monitoring system in Lab 12
# MAGIC - Apply to disaster assessment scenarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC **Challenge**: Extend the change detection system to:
# MAGIC 1. Add more change types (wetland loss, glacier retreat, etc.)
# MAGIC 2. Implement attention mechanisms in the network
# MAGIC 3. Add multi-scale feature fusion
# MAGIC 4. Create change trajectory analysis (3+ time points)
# MAGIC 5. Implement confidence estimation
# MAGIC 6. Export change polygons with attributes
# MAGIC 7. Calculate area statistics per change type
# MAGIC
# MAGIC **Bonus**: Apply to real disaster scenarios using before/after satellite imagery!

# COMMAND ----------

# Your code here
