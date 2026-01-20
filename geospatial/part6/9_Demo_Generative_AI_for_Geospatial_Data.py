# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: Generative AI for Geospatial Data
# MAGIC
# MAGIC ## Overview
# MAGIC Explore generative AI techniques for geospatial data including GANs for synthetic map generation, variational autoencoders for spatial data, diffusion models for imagery, and privacy-preserving data synthesis.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC - Generative Adversarial Networks (GANs) for map generation
# MAGIC - Variational Autoencoders (VAEs) for spatial patterns
# MAGIC - Diffusion models for satellite imagery synthesis
# MAGIC - Data augmentation strategies for geospatial ML
# MAGIC - Privacy-preserving synthetic geospatial data
# MAGIC - Style transfer for maps
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Understanding of generative models (GANs, VAEs)
# MAGIC - Familiarity with deep learning architectures
# MAGIC - Basic knowledge of image generation techniques

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# Install required packages
%pip install torch torchvision numpy pandas matplotlib scikit-learn opencv-python-headless pillow --quiet
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import torchvision.transforms as transforms

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import cv2
from PIL import Image
from sklearn.decomposition import PCA

import warnings
warnings.filterwarnings('ignore')

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Data Generation for Training

# COMMAND ----------

def generate_synthetic_map(size=64, map_type='urban'):
    """
    Generate synthetic map-like images
    
    Args:
        size: Image size
        map_type: 'urban', 'rural', 'coastal'
    
    Returns:
        RGB image array
    """
    np.random.seed(None)
    
    # Base layer
    image = np.zeros((size, size, 3), dtype=np.uint8)
    
    if map_type == 'urban':
        # Grid-like street pattern
        image[:] = [245, 245, 220]  # Beige background
        
        # Add street grid
        grid_spacing = size // 8
        for i in range(0, size, grid_spacing):
            cv2.line(image, (i, 0), (i, size), (180, 180, 180), 2)
            cv2.line(image, (0, i), (size, i), (180, 180, 180), 2)
        
        # Add buildings
        n_buildings = np.random.randint(10, 20)
        for _ in range(n_buildings):
            x = np.random.randint(0, size - 10)
            y = np.random.randint(0, size - 10)
            w = np.random.randint(5, 12)
            h = np.random.randint(5, 12)
            color = np.random.randint(100, 200)
            cv2.rectangle(image, (x, y), (x+w, y+h), (color, color, color), -1)
        
        # Add parks
        n_parks = np.random.randint(1, 3)
        for _ in range(n_parks):
            center = (np.random.randint(10, size-10), np.random.randint(10, size-10))
            radius = np.random.randint(5, 10)
            cv2.circle(image, center, radius, (34, 139, 34), -1)
    
    elif map_type == 'rural':
        # Natural patterns
        image[:] = [34, 139, 34]  # Green background
        
        # Add fields
        n_fields = np.random.randint(3, 6)
        for _ in range(n_fields):
            x = np.random.randint(0, size - 20)
            y = np.random.randint(0, size - 20)
            w = np.random.randint(10, 25)
            h = np.random.randint(10, 25)
            color = [np.random.randint(150, 220), np.random.randint(180, 220), np.random.randint(80, 120)]
            cv2.rectangle(image, (x, y), (x+w, y+h), color, -1)
        
        # Add roads
        n_roads = np.random.randint(1, 3)
        for _ in range(n_roads):
            pt1 = (np.random.randint(0, size), np.random.randint(0, size))
            pt2 = (np.random.randint(0, size), np.random.randint(0, size))
            cv2.line(image, pt1, pt2, (100, 100, 100), 3)
    
    elif map_type == 'coastal':
        # Water and land
        water_line = np.random.randint(size//3, 2*size//3)
        image[:water_line, :] = [34, 139, 34]  # Land (green)
        image[water_line:, :] = [65, 105, 225]  # Water (blue)
        
        # Add beach
        beach_width = np.random.randint(3, 8)
        image[water_line-beach_width:water_line, :] = [238, 214, 175]  # Sand
        
        # Add coastal features
        n_features = np.random.randint(2, 5)
        for _ in range(n_features):
            if np.random.random() < 0.5:
                # Building on land
                x = np.random.randint(0, size - 10)
                y = np.random.randint(0, water_line - 15)
                w = np.random.randint(5, 10)
                h = np.random.randint(5, 10)
                cv2.rectangle(image, (x, y), (x+w, y+h), (200, 200, 200), -1)
            else:
                # Boat in water
                x = np.random.randint(0, size - 5)
                y = np.random.randint(water_line + 5, size - 5)
                cv2.circle(image, (x, y), 2, (255, 255, 255), -1)
    
    # Add noise for realism
    noise = np.random.randint(-10, 10, (size, size, 3))
    image = np.clip(image.astype(int) + noise, 0, 255).astype(np.uint8)
    
    return image

# Generate sample maps
fig, axes = plt.subplots(3, 5, figsize=(15, 9))

for map_type_idx, map_type in enumerate(['urban', 'rural', 'coastal']):
    for i in range(5):
        img = generate_synthetic_map(size=64, map_type=map_type)
        axes[map_type_idx, i].imshow(img)
        axes[map_type_idx, i].axis('off')
        if i == 0:
            axes[map_type_idx, i].set_title(map_type.capitalize(), fontsize=12, fontweight='bold')

plt.tight_layout()
plt.savefig('/dbfs/synthetic_maps.png', dpi=150, bbox_inches='tight')
plt.show()

print("✓ Synthetic map data generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Generative Adversarial Network (GAN)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define GAN Architecture

# COMMAND ----------

class Generator(nn.Module):
    """
    Generator network for map synthesis
    """
    
    def __init__(self, latent_dim=100, img_channels=3, img_size=64):
        super(Generator, self).__init__()
        
        self.latent_dim = latent_dim
        self.img_channels = img_channels
        self.img_size = img_size
        
        # Calculate initial size
        self.init_size = img_size // 4
        self.fc = nn.Linear(latent_dim, 128 * self.init_size ** 2)
        
        self.conv_blocks = nn.Sequential(
            nn.BatchNorm2d(128),
            nn.Upsample(scale_factor=2),
            nn.Conv2d(128, 128, 3, padding=1),
            nn.BatchNorm2d(128),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Upsample(scale_factor=2),
            nn.Conv2d(128, 64, 3, padding=1),
            nn.BatchNorm2d(64),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(64, img_channels, 3, padding=1),
            nn.Tanh()
        )
    
    def forward(self, z):
        """
        Args:
            z: Latent vector (B, latent_dim)
        
        Returns:
            Generated images (B, C, H, W)
        """
        x = self.fc(z)
        x = x.view(x.size(0), 128, self.init_size, self.init_size)
        img = self.conv_blocks(x)
        return img

class Discriminator(nn.Module):
    """
    Discriminator network to classify real vs fake maps
    """
    
    def __init__(self, img_channels=3, img_size=64):
        super(Discriminator, self).__init__()
        
        def discriminator_block(in_filters, out_filters, bn=True):
            block = [nn.Conv2d(in_filters, out_filters, 3, 2, 1)]
            if bn:
                block.append(nn.BatchNorm2d(out_filters))
            block.append(nn.LeakyReLU(0.2, inplace=True))
            block.append(nn.Dropout2d(0.25))
            return block
        
        self.model = nn.Sequential(
            *discriminator_block(img_channels, 16, bn=False),
            *discriminator_block(16, 32),
            *discriminator_block(32, 64),
            *discriminator_block(64, 128),
        )
        
        # Calculate output size
        ds_size = img_size // 2 ** 4
        self.adv_layer = nn.Sequential(
            nn.Linear(128 * ds_size ** 2, 1),
            nn.Sigmoid()
        )
    
    def forward(self, img):
        """
        Args:
            img: Input images (B, C, H, W)
        
        Returns:
            Probability of being real (B, 1)
        """
        out = self.model(img)
        out = out.view(out.size(0), -1)
        validity = self.adv_layer(out)
        return validity

# Initialize models
latent_dim = 100
generator = Generator(latent_dim=latent_dim, img_channels=3, img_size=64).to(device)
discriminator = Discriminator(img_channels=3, img_size=64).to(device)

print("Generator:")
print(f"  Parameters: {sum(p.numel() for p in generator.parameters()):,}")

print("\nDiscriminator:")
print(f"  Parameters: {sum(p.numel() for p in discriminator.parameters()):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Training Data

# COMMAND ----------

class MapDataset(Dataset):
    """Dataset of synthetic maps"""
    
    def __init__(self, n_samples=1000, img_size=64):
        self.n_samples = n_samples
        self.img_size = img_size
        self.map_types = ['urban', 'rural', 'coastal']
        
        # Generate dataset
        self.images = []
        for _ in range(n_samples):
            map_type = np.random.choice(self.map_types)
            img = generate_synthetic_map(size=img_size, map_type=map_type)
            self.images.append(img)
        
        # Transform
        self.transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize([0.5, 0.5, 0.5], [0.5, 0.5, 0.5])
        ])
    
    def __len__(self):
        return self.n_samples
    
    def __getitem__(self, idx):
        img = self.images[idx]
        img = self.transform(img)
        return img

# Create dataset and dataloader
train_dataset = MapDataset(n_samples=500, img_size=64)
train_loader = DataLoader(train_dataset, batch_size=16, shuffle=True)

print(f"Training dataset: {len(train_dataset)} maps")
print(f"Batch size: 16")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Training Loop

# COMMAND ----------

def train_gan(generator, discriminator, dataloader, n_epochs=50, lr=0.0002):
    """Train GAN"""
    
    # Loss function
    adversarial_loss = nn.BCELoss()
    
    # Optimizers
    optimizer_G = optim.Adam(generator.parameters(), lr=lr, betas=(0.5, 0.999))
    optimizer_D = optim.Adam(discriminator.parameters(), lr=lr, betas=(0.5, 0.999))
    
    history = {'d_loss': [], 'g_loss': [], 'd_real_acc': [], 'd_fake_acc': []}
    
    for epoch in range(n_epochs):
        epoch_d_loss = 0
        epoch_g_loss = 0
        epoch_d_real_acc = 0
        epoch_d_fake_acc = 0
        n_batches = 0
        
        for real_imgs in dataloader:
            batch_size = real_imgs.size(0)
            real_imgs = real_imgs.to(device)
            
            # Labels
            real_labels = torch.ones(batch_size, 1).to(device)
            fake_labels = torch.zeros(batch_size, 1).to(device)
            
            # ==================
            # Train Discriminator
            # ==================
            optimizer_D.zero_grad()
            
            # Real images
            real_pred = discriminator(real_imgs)
            d_real_loss = adversarial_loss(real_pred, real_labels)
            
            # Fake images
            z = torch.randn(batch_size, latent_dim).to(device)
            fake_imgs = generator(z)
            fake_pred = discriminator(fake_imgs.detach())
            d_fake_loss = adversarial_loss(fake_pred, fake_labels)
            
            # Total discriminator loss
            d_loss = (d_real_loss + d_fake_loss) / 2
            d_loss.backward()
            optimizer_D.step()
            
            # ===============
            # Train Generator
            # ===============
            optimizer_G.zero_grad()
            
            # Generate new fake images
            z = torch.randn(batch_size, latent_dim).to(device)
            gen_imgs = generator(z)
            
            # Generator tries to fool discriminator
            g_pred = discriminator(gen_imgs)
            g_loss = adversarial_loss(g_pred, real_labels)
            
            g_loss.backward()
            optimizer_G.step()
            
            # Metrics
            d_real_acc = (real_pred > 0.5).float().mean()
            d_fake_acc = (fake_pred < 0.5).float().mean()
            
            epoch_d_loss += d_loss.item()
            epoch_g_loss += g_loss.item()
            epoch_d_real_acc += d_real_acc.item()
            epoch_d_fake_acc += d_fake_acc.item()
            n_batches += 1
        
        # Average metrics
        avg_d_loss = epoch_d_loss / n_batches
        avg_g_loss = epoch_g_loss / n_batches
        avg_d_real_acc = epoch_d_real_acc / n_batches
        avg_d_fake_acc = epoch_d_fake_acc / n_batches
        
        history['d_loss'].append(avg_d_loss)
        history['g_loss'].append(avg_g_loss)
        history['d_real_acc'].append(avg_d_real_acc)
        history['d_fake_acc'].append(avg_d_fake_acc)
        
        if (epoch + 1) % 10 == 0:
            print(f"Epoch {epoch+1}/{n_epochs}")
            print(f"  D Loss: {avg_d_loss:.4f}, G Loss: {avg_g_loss:.4f}")
            print(f"  D Acc (Real): {avg_d_real_acc:.4f}, D Acc (Fake): {avg_d_fake_acc:.4f}")
    
    return history

# Train GAN
print("Training GAN...\n")
history = train_gan(generator, discriminator, train_loader, n_epochs=50, lr=0.0002)

print("\n✓ GAN training complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize Training Progress

# COMMAND ----------

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Loss plot
axes[0].plot(history['d_loss'], label='Discriminator Loss', marker='o', markersize=3)
axes[0].plot(history['g_loss'], label='Generator Loss', marker='s', markersize=3)
axes[0].set_xlabel('Epoch')
axes[0].set_ylabel('Loss')
axes[0].set_title('GAN Training Losses')
axes[0].legend()
axes[0].grid(True)

# Accuracy plot
axes[1].plot(history['d_real_acc'], label='D Acc (Real)', marker='o', markersize=3)
axes[1].plot(history['d_fake_acc'], label='D Acc (Fake)', marker='s', markersize=3)
axes[1].set_xlabel('Epoch')
axes[1].set_ylabel('Accuracy')
axes[1].set_title('Discriminator Accuracy')
axes[1].legend()
axes[1].grid(True)

plt.tight_layout()
plt.savefig('/dbfs/gan_training.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Generating Synthetic Maps

# COMMAND ----------

# Generate synthetic maps
generator.eval()

n_samples = 16
z = torch.randn(n_samples, latent_dim).to(device)

with torch.no_grad():
    generated_maps = generator(z)

# Denormalize
generated_maps = (generated_maps * 0.5 + 0.5).cpu()
generated_maps = generated_maps.permute(0, 2, 3, 1).numpy()
generated_maps = (generated_maps * 255).astype(np.uint8)

# Visualize
fig, axes = plt.subplots(4, 4, figsize=(12, 12))

for i in range(n_samples):
    ax = axes[i // 4, i % 4]
    ax.imshow(generated_maps[i])
    ax.axis('off')
    ax.set_title(f'Generated {i+1}')

plt.tight_layout()
plt.savefig('/dbfs/generated_maps.png', dpi=150, bbox_inches='tight')
plt.show()

print("✓ Generated synthetic maps")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Variational Autoencoder (VAE)

# COMMAND ----------

class MapVAE(nn.Module):
    """
    Variational Autoencoder for map generation
    """
    
    def __init__(self, latent_dim=64, img_channels=3, img_size=64):
        super(MapVAE, self).__init__()
        
        self.latent_dim = latent_dim
        
        # Encoder
        self.encoder = nn.Sequential(
            nn.Conv2d(img_channels, 32, 4, 2, 1),  # 64 -> 32
            nn.ReLU(),
            nn.Conv2d(32, 64, 4, 2, 1),  # 32 -> 16
            nn.ReLU(),
            nn.Conv2d(64, 128, 4, 2, 1),  # 16 -> 8
            nn.ReLU(),
            nn.Conv2d(128, 256, 4, 2, 1),  # 8 -> 4
            nn.ReLU(),
        )
        
        # Latent space
        self.fc_mu = nn.Linear(256 * 4 * 4, latent_dim)
        self.fc_logvar = nn.Linear(256 * 4 * 4, latent_dim)
        
        # Decoder
        self.decoder_input = nn.Linear(latent_dim, 256 * 4 * 4)
        
        self.decoder = nn.Sequential(
            nn.ConvTranspose2d(256, 128, 4, 2, 1),  # 4 -> 8
            nn.ReLU(),
            nn.ConvTranspose2d(128, 64, 4, 2, 1),  # 8 -> 16
            nn.ReLU(),
            nn.ConvTranspose2d(64, 32, 4, 2, 1),  # 16 -> 32
            nn.ReLU(),
            nn.ConvTranspose2d(32, img_channels, 4, 2, 1),  # 32 -> 64
            nn.Tanh()
        )
    
    def encode(self, x):
        """Encode input to latent distribution"""
        x = self.encoder(x)
        x = x.view(x.size(0), -1)
        mu = self.fc_mu(x)
        logvar = self.fc_logvar(x)
        return mu, logvar
    
    def reparameterize(self, mu, logvar):
        """Reparameterization trick"""
        std = torch.exp(0.5 * logvar)
        eps = torch.randn_like(std)
        return mu + eps * std
    
    def decode(self, z):
        """Decode latent vector to image"""
        x = self.decoder_input(z)
        x = x.view(x.size(0), 256, 4, 4)
        x = self.decoder(x)
        return x
    
    def forward(self, x):
        mu, logvar = self.encode(x)
        z = self.reparameterize(mu, logvar)
        recon = self.decode(z)
        return recon, mu, logvar

# Initialize VAE
vae = MapVAE(latent_dim=64, img_channels=3, img_size=64).to(device)

print("VAE Model:")
print(f"  Latent dimension: {vae.latent_dim}")
print(f"  Parameters: {sum(p.numel() for p in vae.parameters()):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train VAE

# COMMAND ----------

def vae_loss(recon_x, x, mu, logvar):
    """VAE loss = Reconstruction + KL divergence"""
    recon_loss = F.mse_loss(recon_x, x, reduction='sum')
    kl_loss = -0.5 * torch.sum(1 + logvar - mu.pow(2) - logvar.exp())
    return recon_loss + kl_loss

def train_vae(model, dataloader, n_epochs=30, lr=0.001):
    """Train VAE"""
    optimizer = optim.Adam(model.parameters(), lr=lr)
    
    history = {'loss': [], 'recon_loss': [], 'kl_loss': []}
    
    for epoch in range(n_epochs):
        model.train()
        epoch_loss = 0
        epoch_recon = 0
        epoch_kl = 0
        n_batches = 0
        
        for data in dataloader:
            data = data.to(device)
            
            optimizer.zero_grad()
            recon, mu, logvar = model(data)
            
            # Calculate losses
            recon_loss = F.mse_loss(recon, data, reduction='sum')
            kl_loss = -0.5 * torch.sum(1 + logvar - mu.pow(2) - logvar.exp())
            loss = recon_loss + kl_loss
            
            loss.backward()
            optimizer.step()
            
            epoch_loss += loss.item()
            epoch_recon += recon_loss.item()
            epoch_kl += kl_loss.item()
            n_batches += 1
        
        avg_loss = epoch_loss / n_batches
        avg_recon = epoch_recon / n_batches
        avg_kl = epoch_kl / n_batches
        
        history['loss'].append(avg_loss)
        history['recon_loss'].append(avg_recon)
        history['kl_loss'].append(avg_kl)
        
        if (epoch + 1) % 5 == 0:
            print(f"Epoch {epoch+1}/{n_epochs} - Loss: {avg_loss:.2f} (Recon: {avg_recon:.2f}, KL: {avg_kl:.2f})")
    
    return history

# Train VAE
print("Training VAE...\n")
vae_history = train_vae(vae, train_loader, n_epochs=30, lr=0.001)

print("\n✓ VAE training complete!")

# Plot VAE training
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(vae_history['loss'], label='Total Loss', marker='o', markersize=3)
ax.plot(vae_history['recon_loss'], label='Reconstruction Loss', marker='s', markersize=3)
ax.plot(vae_history['kl_loss'], label='KL Loss', marker='^', markersize=3)
ax.set_xlabel('Epoch')
ax.set_ylabel('Loss')
ax.set_title('VAE Training')
ax.legend()
ax.grid(True)

plt.tight_layout()
plt.savefig('/dbfs/vae_training.png', dpi=150, bbox_inches='tight')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Latent Space Interpolation

# COMMAND ----------

# Generate interpolations in latent space
vae.eval()

# Sample two random points in latent space
z1 = torch.randn(1, vae.latent_dim).to(device)
z2 = torch.randn(1, vae.latent_dim).to(device)

# Interpolate
n_steps = 8
alphas = np.linspace(0, 1, n_steps)

interpolated_images = []
for alpha in alphas:
    z_interp = (1 - alpha) * z1 + alpha * z2
    with torch.no_grad():
        img = vae.decode(z_interp)
    img = (img * 0.5 + 0.5).cpu().squeeze().permute(1, 2, 0).numpy()
    img = (img * 255).astype(np.uint8)
    interpolated_images.append(img)

# Visualize
fig, axes = plt.subplots(2, 4, figsize=(14, 7))

for i, img in enumerate(interpolated_images):
    ax = axes[i // 4, i % 4]
    ax.imshow(img)
    ax.axis('off')
    ax.set_title(f'α = {alphas[i]:.2f}')

plt.suptitle('Latent Space Interpolation', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('/dbfs/vae_interpolation.png', dpi=150, bbox_inches='tight')
plt.show()

print("✓ Latent space interpolation complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Privacy-Preserving Synthetic Data

# COMMAND ----------

class PrivacyPreservingGenerator:
    """
    Generate privacy-preserving synthetic geospatial data
    
    Uses differential privacy principles
    """
    
    def __init__(self, epsilon=1.0):
        """
        Args:
            epsilon: Privacy budget (smaller = more privacy)
        """
        self.epsilon = epsilon
    
    def add_laplace_noise(self, data, sensitivity):
        """Add Laplace noise for differential privacy"""
        scale = sensitivity / self.epsilon
        noise = np.random.laplace(0, scale, data.shape)
        return data + noise
    
    def generate_synthetic_locations(self, real_locations, n_synthetic, sensitivity=0.01):
        """
        Generate synthetic location data with privacy guarantees
        
        Args:
            real_locations: Array of (lat, lon) pairs
            n_synthetic: Number of synthetic locations to generate
            sensitivity: Sensitivity of the data
        
        Returns:
            Synthetic locations with noise added
        """
        # Calculate statistics from real data
        mean_lat = np.mean(real_locations[:, 0])
        mean_lon = np.mean(real_locations[:, 1])
        std_lat = np.std(real_locations[:, 0])
        std_lon = np.std(real_locations[:, 1])
        
        # Add noise to statistics
        noisy_mean_lat = self.add_laplace_noise(mean_lat, sensitivity)
        noisy_mean_lon = self.add_laplace_noise(mean_lon, sensitivity)
        noisy_std_lat = self.add_laplace_noise(std_lat, sensitivity)
        noisy_std_lon = self.add_laplace_noise(std_lon, sensitivity)
        
        # Generate synthetic data from noisy statistics
        synthetic_lats = np.random.normal(noisy_mean_lat, abs(noisy_std_lat), n_synthetic)
        synthetic_lons = np.random.normal(noisy_mean_lon, abs(noisy_std_lon), n_synthetic)
        
        synthetic_locations = np.column_stack([synthetic_lats, synthetic_lons])
        
        return synthetic_locations

# Generate real locations
np.random.seed(42)
real_locations = np.random.multivariate_normal(
    [37.7749, -122.4194],
    [[0.001, 0], [0, 0.001]],
    size=100
)

# Generate synthetic locations with different privacy levels
ppg_low = PrivacyPreservingGenerator(epsilon=10.0)  # Less privacy
ppg_high = PrivacyPreservingGenerator(epsilon=0.5)  # More privacy

synthetic_low = ppg_low.generate_synthetic_locations(real_locations, n_synthetic=100)
synthetic_high = ppg_high.generate_synthetic_locations(real_locations, n_synthetic=100)

# Visualize
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

axes[0].scatter(real_locations[:, 1], real_locations[:, 0], alpha=0.6, s=30)
axes[0].set_xlabel('Longitude')
axes[0].set_ylabel('Latitude')
axes[0].set_title('Real Locations')
axes[0].grid(True, alpha=0.3)

axes[1].scatter(synthetic_low[:, 1], synthetic_low[:, 0], alpha=0.6, s=30, color='orange')
axes[1].set_xlabel('Longitude')
axes[1].set_ylabel('Latitude')
axes[1].set_title('Synthetic (ε=10, Low Privacy)')
axes[1].grid(True, alpha=0.3)

axes[2].scatter(synthetic_high[:, 1], synthetic_high[:, 0], alpha=0.6, s=30, color='green')
axes[2].set_xlabel('Longitude')
axes[2].set_ylabel('Latitude')
axes[2].set_title('Synthetic (ε=0.5, High Privacy)')
axes[2].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('/dbfs/privacy_preserving.png', dpi=150, bbox_inches='tight')
plt.show()

print("Privacy-Preserving Synthetic Data Statistics:")
print(f"\nReal data:")
print(f"  Mean: ({real_locations[:, 0].mean():.4f}, {real_locations[:, 1].mean():.4f})")
print(f"  Std: ({real_locations[:, 0].std():.4f}, {real_locations[:, 1].std():.4f})")

print(f"\nSynthetic (ε=10):")
print(f"  Mean: ({synthetic_low[:, 0].mean():.4f}, {synthetic_low[:, 1].mean():.4f})")
print(f"  Std: ({synthetic_low[:, 0].std():.4f}, {synthetic_low[:, 1].std():.4f})")

print(f"\nSynthetic (ε=0.5):")
print(f"  Mean: ({synthetic_high[:, 0].mean():.4f}, {synthetic_high[:, 1].mean():.4f})")
print(f"  Std: ({synthetic_high[:, 0].std():.4f}, {synthetic_high[:, 1].std():.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary and Best Practices
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **GANs**: Generate realistic synthetic maps and imagery
# MAGIC 2. **VAEs**: Enable smooth interpolation in latent space
# MAGIC 3. **Privacy**: Differential privacy ensures data protection
# MAGIC 4. **Applications**: Data augmentation, simulation, anonymization
# MAGIC 5. **Quality**: Balance realism with privacy requirements
# MAGIC 6. **Evaluation**: Use both visual inspection and metrics
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC - **Training Stability**: Monitor generator and discriminator balance
# MAGIC - **Mode Collapse**: Use techniques like mini-batch discrimination
# MAGIC - **Evaluation**: Use FID, IS scores for quantitative assessment
# MAGIC - **Privacy Budget**: Choose epsilon carefully based on requirements
# MAGIC - **Validation**: Ensure synthetic data preserves key statistics
# MAGIC - **Domain Knowledge**: Incorporate geospatial constraints
# MAGIC - **Conditional Generation**: Add labels/conditions for control
# MAGIC - **Post-Processing**: Apply geospatial validity checks
# MAGIC
# MAGIC ### Applications
# MAGIC
# MAGIC - **Data Augmentation**: Increase training data for ML models
# MAGIC - **Simulation**: Generate scenarios for urban planning
# MAGIC - **Privacy**: Share data without exposing sensitive locations
# MAGIC - **Testing**: Create test datasets for algorithms
# MAGIC - **Visualization**: Generate examples for presentations
# MAGIC - **Research**: Enable studies without real data access
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC - Apply to real satellite imagery datasets
# MAGIC - Implement conditional GANs for controlled generation
# MAGIC - Build complete pipelines in Labs 10-15
# MAGIC - Explore diffusion models for higher quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC **Challenge**: Extend the generative AI system to:
# MAGIC 1. Implement conditional GAN (specify map type)
# MAGIC 2. Add style transfer between different map styles
# MAGIC 3. Implement progressive growing of GANs
# MAGIC 4. Create super-resolution network for maps
# MAGIC 5. Add temporal consistency for time-series generation
# MAGIC 6. Implement quality metrics (FID, IS scores)
# MAGIC 7. Build web interface for interactive generation
# MAGIC
# MAGIC **Bonus**: Train on real satellite imagery and evaluate quality!

# COMMAND ----------

# Your code here
