# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Land Cover Classification
# MAGIC
# MAGIC ## Overview
# MAGIC Learn semantic segmentation techniques for land cover classification using deep learning. This demo covers U-Net and DeepLab architectures, training on satellite imagery, post-processing, and accuracy assessment.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Semantic segmentation architectures for land cover
# MAGIC - Multi-class classification with U-Net and DeepLabV3+
# MAGIC - Training strategies for imbalanced classes
# MAGIC - Post-processing techniques (CRF, morphological operations)
# MAGIC - Accuracy assessment and validation
# MAGIC - Exporting results to GeoTIFF and vector formats
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Completion of Demo 4 (Satellite Imagery Deep Learning)
# MAGIC - Understanding of semantic segmentation
# MAGIC - Familiarity with land cover classification concepts

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

from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from skimage import morphology, measure
import geopandas as gpd
from shapely.geometry import shape, Polygon

import warnings
warnings.filterwarnings('ignore')

# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# Define land cover classes
LAND_COVER_CLASSES = {
    0: 'Background',
    1: 'Water',
    2: 'Forest',
    3: 'Grassland',
    4: 'Cropland',
    5: 'Urban',
    6: 'Bare Ground'
}

N_CLASSES = len(LAND_COVER_CLASSES)

# Color mapping for visualization
CLASS_COLORS = {
    0: [0, 0, 0],        # Background - Black
    1: [0, 0, 255],      # Water - Blue
    2: [0, 128, 0],      # Forest - Dark Green
    3: [144, 238, 144],  # Grassland - Light Green
    4: [255, 255, 0],    # Cropland - Yellow
    5: [255, 0, 0],      # Urban - Red
    6: [139, 69, 19]     # Bare Ground - Brown
}

print(f"Land Cover Classes: {N_CLASSES}")
for class_id, class_name in LAND_COVER_CLASSES.items():
    print(f"  {class_id}: {class_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Generating Synthetic Land Cover Data

# COMMAND ----------

def generate_land_cover_scene(size=256, complexity='medium'):
    """
    Generate synthetic land cover scene
    
    Args:
        size: Image size
        complexity: Scene complexity ('simple', 'medium', 'complex')
    
    Returns:
        image: RGB image (H, W, 3)
        mask: Land cover mask (H, W) with class IDs
    """
    np.random.seed(None)
    
    # Initialize
    image = np.zeros((size, size, 3), dtype=np.uint8)
    mask = np.zeros((size, size), dtype=np.uint8)
    
    # Create base terrain (grassland/forest)
    base_class = np.random.choice([2, 3])  # Forest or Grassland
    mask[:] = base_class
    
    if base_class == 2:  # Forest
        image[:] = [34, 139, 34]  # Forest green
    else:  # Grassland
        image[:] = [144, 238, 144]  # Light green
    
    # Add texture/noise
    noise = np.random.randint(-30, 30, (size, size, 3))
    image = np.clip(image.astype(int) + noise, 0, 255).astype(np.uint8)
    
    # Add water bodies
    n_water = np.random.randint(1, 4) if complexity in ['medium', 'complex'] else 1
    for _ in range(n_water):
        # Random water body
        center_x = np.random.randint(size // 4, 3 * size // 4)
        center_y = np.random.randint(size // 4, 3 * size // 4)
        radius = np.random.randint(20, 50)
        
        cv2.circle(image, (center_x, center_y), radius, (65, 105, 225), -1)  # Royal blue
        cv2.circle(mask, (center_x, center_y), radius, 1, -1)  # Water class
    
    # Add cropland patches
    if complexity in ['medium', 'complex']:
        n_crops = np.random.randint(2, 5)
        for _ in range(n_crops):
            x = np.random.randint(10, size - 50)
            y = np.random.randint(10, size - 50)
            w = np.random.randint(30, 60)
            h = np.random.randint(30, 60)
            
            # Cropland color (yellow-brown)
            crop_color = [np.random.randint(200, 255), np.random.randint(200, 220), np.random.randint(100, 150)]
            cv2.rectangle(image, (x, y), (x+w, y+h), crop_color, -1)
            cv2.rectangle(mask, (x, y), (x+w, y+h), 4, -1)  # Cropland class
    
    # Add urban areas
    if complexity in ['medium', 'complex']:
        n_urban = np.random.randint(1, 3)
        for _ in range(n_urban):
            x = np.random.randint(10, size - 60)
            y = np.random.randint(10, size - 60)
            
            # Urban cluster (buildings)
            n_buildings = np.random.randint(3, 8)
            for _ in range(n_buildings):
                bx = x + np.random.randint(-20, 20)
                by = y + np.random.randint(-20, 20)
                bw = np.random.randint(10, 25)
                bh = np.random.randint(10, 25)
                
                if 0 <= bx < size-bw and 0 <= by < size-bh:
                    color = np.random.randint(180, 220)
                    cv2.rectangle(image, (bx, by), (bx+bw, by+bh), (color, color, color), -1)
                    cv2.rectangle(mask, (bx, by), (bx+bw, by+bh), 5, -1)  # Urban class
    
    # Add bare ground patches
    if complexity == 'complex':
        n_bare = np.random.randint(1, 3)
        for _ in range(n_bare):
            x = np.random.randint(10, size - 40)
            y = np.random.randint(10, size - 40)
            w = np.random.randint(20, 40)
            h = np.random.randint(20, 40)
            
            bare_color = [np.random.randint(150, 180), np.random.randint(120, 150), np.random.randint(80, 120)]
            cv2.rectangle(image, (x, y), (x+w, y+h), bare_color, -1)
            cv2.rectangle(mask, (x, y), (x+w, y+h), 6, -1)  # Bare ground class
    
    return image, mask

# Generate sample scenes
fig, axes = plt.subplots(3, 4, figsize=(16, 12))

for i in range(4):
    complexity = ['simple', 'medium', 'complex'][i % 3]
    image, mask = generate_land_cover_scene(size=256, complexity=complexity)
    
    # Convert mask to color for visualization
    mask_color = np.zeros((256, 256, 3), dtype=np.uint8)
    for class_id, color in CLASS_COLORS.items():
        mask_color[mask == class_id] = color
    
    axes[0, i].imshow(image)
    axes[0, i].set_title(f'Scene {i+1} ({complexity})')
    axes[0, i].axis('off')
    
    axes[1, i].imshow(mask, cmap='tab10', vmin=0, vmax=N_CLASSES-1)
    axes[1, i].set_title('Class IDs')
    axes[1, i].axis('off')
    
    axes[2, i].imshow(mask_color)
    axes[2, i].set_title('Colored Mask')
    axes[2, i].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/land_cover_samples.png', dpi=150, bbox_inches='tight')
plt.show()

print("✓ Sample land cover scenes generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Dataset Preparation

# COMMAND ----------

class LandCoverDataset(Dataset):
    """PyTorch Dataset for land cover classification"""
    
    def __init__(self, images, masks, transform=None):
        self.images = images
        self.masks = masks
        self.transform = transform
    
    def __len__(self):
        return len(self.images)
    
    def __getitem__(self, idx):
        image = self.images[idx]
        mask = self.masks[idx]
        
        if self.transform:
            transformed = self.transform(image=image, mask=mask)
            image = transformed['image']
            mask = transformed['mask']
        
        if not isinstance(mask, torch.Tensor):
            mask = torch.from_numpy(mask).long()
        
        return image, mask

# Augmentation pipelines
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
    A.RandomBrightnessContrast(brightness_limit=0.2, contrast_limit=0.2, p=0.3),
    A.HueSaturationValue(hue_shift_limit=20, sat_shift_limit=30, val_shift_limit=20, p=0.3),
    A.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225)),
    ToTensorV2()
])

val_transform = A.Compose([
    A.Resize(256, 256),
    A.Normalize(mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225)),
    ToTensorV2()
])

# Generate dataset
print("Generating land cover dataset...")
n_samples = 300

all_images = []
all_masks = []

for i in range(n_samples):
    complexity = np.random.choice(['simple', 'medium', 'complex'], p=[0.2, 0.5, 0.3])
    image, mask = generate_land_cover_scene(size=256, complexity=complexity)
    all_images.append(image)
    all_masks.append(mask)
    
    if (i + 1) % 100 == 0:
        print(f"  Generated {i+1}/{n_samples} samples")

# Class distribution
all_masks_flat = np.concatenate([m.flatten() for m in all_masks])
unique, counts = np.unique(all_masks_flat, return_counts=True)
class_distribution = dict(zip(unique, counts))

print(f"\nClass distribution (pixels):")
for class_id in range(N_CLASSES):
    if class_id in class_distribution:
        count = class_distribution[class_id]
        pct = 100 * count / len(all_masks_flat)
        print(f"  {LAND_COVER_CLASSES[class_id]}: {count:,} ({pct:.2f}%)")

# Split dataset
from sklearn.model_selection import train_test_split
train_images, val_images, train_masks, val_masks = train_test_split(
    all_images, all_masks, test_size=0.2, random_state=42
)

print(f"\nTrain samples: {len(train_images)}")
print(f"Val samples: {len(val_images)}")

# Create datasets and dataloaders
train_dataset = LandCoverDataset(train_images, train_masks, transform=train_transform)
val_dataset = LandCoverDataset(val_images, val_masks, transform=val_transform)

batch_size = 8
train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True, num_workers=0)
val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False, num_workers=0)

print(f"✓ DataLoaders created (batch_size={batch_size})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Model Architecture

# COMMAND ----------

# Create U-Net with ResNet50 encoder
model = smp.Unet(
    encoder_name="resnet50",
    encoder_weights="imagenet",
    in_channels=3,
    classes=N_CLASSES,
    activation=None
)

model = model.to(device)

print("Model Summary:")
print(f"  Architecture: U-Net")
print(f"  Encoder: ResNet50 (ImageNet pretrained)")
print(f"  Input: 3 channels (RGB)")
print(f"  Output: {N_CLASSES} classes")
print(f"  Parameters: {sum(p.numel() for p in model.parameters()):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Loss Function with Class Weights

# COMMAND ----------

# Calculate class weights to handle imbalance
class_counts = np.array([class_distribution.get(i, 1) for i in range(N_CLASSES)])
class_weights = 1.0 / (class_counts + 1e-6)
class_weights = class_weights / class_weights.sum() * N_CLASSES
class_weights = torch.FloatTensor(class_weights).to(device)

print("Class weights:")
for i, (class_id, class_name) in enumerate(LAND_COVER_CLASSES.items()):
    print(f"  {class_name}: {class_weights[i]:.4f}")

# Loss function with class weights
criterion = nn.CrossEntropyLoss(weight=class_weights)

# Optimizer
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Learning rate scheduler
scheduler = optim.lr_scheduler.ReduceLROnPlateau(
    optimizer, mode='min', factor=0.5, patience=3, verbose=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Training

# COMMAND ----------

def calculate_metrics(pred, target, n_classes):
    """Calculate IoU and pixel accuracy"""
    pred = pred.view(-1).cpu().numpy()
    target = target.view(-1).cpu().numpy()
    
    # Overall accuracy
    accuracy = accuracy_score(target, pred)
    
    # Per-class IoU
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
    
    # Mean IoU (excluding NaN)
    valid_ious = [iou for iou in ious if not np.isnan(iou)]
    mean_iou = np.mean(valid_ious) if valid_ious else 0.0
    
    return accuracy, mean_iou, ious

def train_epoch(model, dataloader, criterion, optimizer, device):
    """Train one epoch"""
    model.train()
    running_loss = 0.0
    running_acc = 0.0
    running_iou = 0.0
    n_batches = 0
    
    for images, masks in dataloader:
        images = images.to(device)
        masks = masks.to(device)
        
        optimizer.zero_grad()
        outputs = model(images)
        loss = criterion(outputs, masks)
        loss.backward()
        optimizer.step()
        
        preds = torch.argmax(outputs, dim=1)
        acc, mean_iou, _ = calculate_metrics(preds, masks, N_CLASSES)
        
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
        for images, masks in dataloader:
            images = images.to(device)
            masks = masks.to(device)
            
            outputs = model(images)
            loss = criterion(outputs, masks)
            
            preds = torch.argmax(outputs, dim=1)
            acc, mean_iou, _ = calculate_metrics(preds, masks, N_CLASSES)
            
            running_loss += loss.item()
            running_acc += acc
            running_iou += mean_iou
            n_batches += 1
    
    return running_loss / n_batches, running_acc / n_batches, running_iou / n_batches

# Training loop
n_epochs = 15
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

# COMMAND ----------

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
axes[1].set_title('Pixel Accuracy')
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
plt.savefig('/dbfs/land_cover_training.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Inference and Visualization

# COMMAND ----------

def predict_image(model, image, transform, device):
    """Predict land cover for image"""
    model.eval()
    
    transformed = transform(image=image, mask=np.zeros_like(image[:, :, 0]))
    image_tensor = transformed['image'].unsqueeze(0).to(device)
    
    with torch.no_grad():
        output = model(image_tensor)
        pred = torch.argmax(output, dim=1).squeeze().cpu().numpy()
    
    return pred

def mask_to_color(mask):
    """Convert class mask to RGB color image"""
    h, w = mask.shape
    color_mask = np.zeros((h, w, 3), dtype=np.uint8)
    
    for class_id, color in CLASS_COLORS.items():
        color_mask[mask == class_id] = color
    
    return color_mask

# Visualize predictions
sample_indices = np.random.choice(len(val_images), 6, replace=False)

fig, axes = plt.subplots(6, 3, figsize=(12, 24))

for idx, sample_idx in enumerate(sample_indices):
    image = val_images[sample_idx]
    gt_mask = val_masks[sample_idx]
    
    pred_mask = predict_image(model, image, val_transform, device)
    
    # Calculate metrics
    acc, mean_iou, class_ious = calculate_metrics(
        torch.from_numpy(pred_mask).flatten(),
        torch.from_numpy(gt_mask).flatten(),
        N_CLASSES
    )
    
    # Visualize
    axes[idx, 0].imshow(image)
    axes[idx, 0].set_title('Input Image')
    axes[idx, 0].axis('off')
    
    axes[idx, 1].imshow(mask_to_color(gt_mask))
    axes[idx, 1].set_title('Ground Truth')
    axes[idx, 1].axis('off')
    
    axes[idx, 2].imshow(mask_to_color(pred_mask))
    axes[idx, 2].set_title(f'Prediction\nAcc: {acc:.3f}, IoU: {mean_iou:.3f}')
    axes[idx, 2].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/land_cover_predictions.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Detailed Accuracy Assessment

# COMMAND ----------

# Evaluate on full validation set
print("Evaluating on validation set...")

all_preds = []
all_targets = []

model.eval()
with torch.no_grad():
    for images, masks in val_loader:
        images = images.to(device)
        outputs = model(images)
        preds = torch.argmax(outputs, dim=1)
        
        all_preds.append(preds.cpu().numpy())
        all_targets.append(masks.cpu().numpy())

all_preds = np.concatenate([p.flatten() for p in all_preds])
all_targets = np.concatenate([t.flatten() for t in all_targets])

# Confusion matrix
conf_matrix = confusion_matrix(all_targets, all_preds, labels=list(range(N_CLASSES)))

# Per-class metrics
print("\nPer-class metrics:")
print("=" * 80)
print(f"{'Class':<15} {'Precision':<12} {'Recall':<12} {'F1-Score':<12} {'IoU':<12}")
print("=" * 80)

for class_id in range(N_CLASSES):
    class_name = LAND_COVER_CLASSES[class_id]
    
    # Calculate metrics
    tp = conf_matrix[class_id, class_id]
    fp = conf_matrix[:, class_id].sum() - tp
    fn = conf_matrix[class_id, :].sum() - tp
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    iou = tp / (tp + fp + fn) if (tp + fp + fn) > 0 else 0
    
    print(f"{class_name:<15} {precision:<12.4f} {recall:<12.4f} {f1:<12.4f} {iou:<12.4f}")

print("=" * 80)

# Overall metrics
overall_acc = accuracy_score(all_targets, all_preds)
print(f"\nOverall Accuracy: {overall_acc:.4f}")

# Plot confusion matrix
fig, ax = plt.subplots(figsize=(10, 8))
im = ax.imshow(conf_matrix, cmap='Blues')

ax.set_xticks(np.arange(N_CLASSES))
ax.set_yticks(np.arange(N_CLASSES))
ax.set_xticklabels([LAND_COVER_CLASSES[i] for i in range(N_CLASSES)], rotation=45, ha='right')
ax.set_yticklabels([LAND_COVER_CLASSES[i] for i in range(N_CLASSES)])

# Add text annotations
for i in range(N_CLASSES):
    for j in range(N_CLASSES):
        text = ax.text(j, i, f'{conf_matrix[i, j]}',
                      ha="center", va="center", color="black" if conf_matrix[i, j] < conf_matrix.max()/2 else "white")

ax.set_title('Confusion Matrix')
ax.set_xlabel('Predicted Class')
ax.set_ylabel('True Class')
fig.colorbar(im, ax=ax)

plt.tight_layout()
plt.savefig('/dbfs/confusion_matrix.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Post-Processing

# COMMAND ----------

def post_process_prediction(pred_mask, min_size=50):
    """
    Post-process prediction mask
    
    Args:
        pred_mask: Raw prediction mask
        min_size: Minimum region size to keep
    
    Returns:
        Cleaned prediction mask
    """
    cleaned_mask = pred_mask.copy()
    
    # For each class, remove small isolated regions
    for class_id in range(N_CLASSES):
        binary_mask = (pred_mask == class_id).astype(np.uint8)
        
        # Remove small objects
        binary_mask = morphology.remove_small_objects(
            binary_mask.astype(bool), 
            min_size=min_size
        ).astype(np.uint8)
        
        # Fill small holes
        binary_mask = morphology.remove_small_holes(
            binary_mask.astype(bool), 
            area_threshold=min_size
        ).astype(np.uint8)
        
        # Update cleaned mask
        cleaned_mask[binary_mask == 1] = class_id
    
    # Morphological closing to smooth boundaries
    kernel = np.ones((3, 3), np.uint8)
    for class_id in range(N_CLASSES):
        binary_mask = (cleaned_mask == class_id).astype(np.uint8)
        binary_mask = cv2.morphologyEx(binary_mask, cv2.MORPH_CLOSE, kernel)
        cleaned_mask[binary_mask == 1] = class_id
    
    return cleaned_mask

# Demonstrate post-processing
sample_idx = sample_indices[0]
image = val_images[sample_idx]
pred_mask = predict_image(model, image, val_transform, device)
cleaned_mask = post_process_prediction(pred_mask, min_size=30)

fig, axes = plt.subplots(1, 3, figsize=(15, 5))

axes[0].imshow(image)
axes[0].set_title('Input Image')
axes[0].axis('off')

axes[1].imshow(mask_to_color(pred_mask))
axes[1].set_title('Raw Prediction')
axes[1].axis('off')

axes[2].imshow(mask_to_color(cleaned_mask))
axes[2].set_title('Post-Processed')
axes[2].axis('off')

plt.tight_layout()
plt.savefig('/dbfs/post_processing.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Multi-Class Segmentation**: Handle multiple land cover classes simultaneously
# MAGIC 2. **Class Imbalance**: Use weighted loss functions for imbalanced data
# MAGIC 3. **Post-Processing**: Clean predictions with morphological operations
# MAGIC 4. **Validation**: Comprehensive metrics (precision, recall, F1, IoU) per class
# MAGIC 5. **Transfer Learning**: Pre-trained encoders significantly improve performance
# MAGIC 6. **Augmentation**: Critical for generalizing from limited training data
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - Use class weights to handle imbalanced datasets
# MAGIC - Implement strong data augmentation strategies
# MAGIC - Monitor per-class metrics, not just overall accuracy
# MAGIC - Post-process to remove artifacts and smooth boundaries
# MAGIC - Validate on diverse geographic regions
# MAGIC - Consider multi-scale inference for better results
# MAGIC - Export results in standard GIS formats
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Learn change detection techniques in Demo 6
# MAGIC - Apply to real satellite imagery
# MAGIC - Build environmental monitoring system in Lab 12

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC **Challenge**: Extend this classification system to:
# MAGIC 1. Add more land cover classes (wetlands, snow/ice, etc.)
# MAGIC 2. Implement DeepLabV3+ architecture and compare
# MAGIC 3. Add multi-temporal analysis (seasonal changes)
# MAGIC 4. Implement test-time augmentation
# MAGIC 5. Export results to GeoTIFF with proper georeferencing
# MAGIC 6. Calculate area statistics per class
# MAGIC
# MAGIC **Bonus**: Use real Sentinel-2 or Landsat imagery with ground truth labels!

# COMMAND ----------

# Your code here
