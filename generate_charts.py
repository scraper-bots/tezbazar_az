"""
Tezbazar Real Estate Market Analysis - Chart Generation Script
This script analyzes the real estate listings data and generates business insights visualizations.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set style for professional business charts
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# Create charts directory
CHARTS_DIR = Path('charts')
CHARTS_DIR.mkdir(exist_ok=True)

# Load the dataset
print("Loading dataset...")
df = pd.read_csv('tezbazar_async_results.csv')
print(f"Dataset loaded: {len(df)} listings")

# Data preprocessing
print("\nPreprocessing data...")

# Clean price column - extract numeric values
def clean_price(price_str):
    if pd.isna(price_str):
        return None
    # Extract numbers and convert to float
    price_str = str(price_str).replace(' ', '').replace(',', '')
    match = re.search(r'(\d+)', price_str)
    if match:
        return float(match.group(1))
    return None

df['price_clean'] = df['price'].apply(clean_price)

# Clean area column
def clean_area(area_str):
    if pd.isna(area_str):
        return None
    area_str = str(area_str).replace(',', '.')
    match = re.search(r'(\d+\.?\d*)', area_str)
    if match:
        return float(match.group(1))
    return None

df['area_clean'] = df['area'].apply(clean_area)

# Parse date
df['date_posted_clean'] = pd.to_datetime(df['date_posted'], format='%d.%m.%Y', errors='coerce')

# Extract city from location
def extract_city(location_str):
    if pd.isna(location_str):
        return 'Unknown'
    if 'Bakı şəhəri' in str(location_str):
        return 'Bakı'
    elif 'Xırdalan' in str(location_str):
        return 'Xırdalan'
    elif 'Sumqayıt' in str(location_str):
        return 'Sumqayıt'
    elif 'Siyəzən' in str(location_str):
        return 'Siyəzən'
    elif 'Kürdəmir' in str(location_str):
        return 'Kürdəmir'
    else:
        return 'Other'

df['city'] = df['location'].apply(extract_city)

# Clean category names for better readability
category_mapping = {
    'Mənzillər': 'Apartments',
    'Obyekt / Ofis': 'Commercial/Office',
    'Torpaq satqısı': 'Land',
    'Həyət evləri , Villalar': 'Houses/Villas',
    'Kirayə evlər': 'Rentals'
}
df['category_clean'] = df['category'].map(category_mapping).fillna(df['category'])

print(f"Data preprocessing complete. Valid prices: {df['price_clean'].notna().sum()}")

# ============================================================================
# CHART 1: Average Price by Property Category
# ============================================================================
print("\nGenerating Chart 1: Average Price by Property Category...")
fig, ax = plt.subplots(figsize=(12, 6))

avg_price_by_category = df.groupby('category_clean')['price_clean'].mean().sort_values(ascending=False)

colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E']
bars = ax.barh(avg_price_by_category.index, avg_price_by_category.values, color=colors)

# Add value labels
for i, (idx, val) in enumerate(avg_price_by_category.items()):
    ax.text(val + 5000, i, f'{val:,.0f} AZN', va='center', fontweight='bold')

ax.set_xlabel('Average Price (AZN)', fontsize=12, fontweight='bold')
ax.set_title('Average Listing Price by Property Category', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '01_avg_price_by_category.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 1 saved")

# ============================================================================
# CHART 2: Property Category Distribution (Inventory Mix)
# ============================================================================
print("Generating Chart 2: Property Category Distribution...")
fig, ax = plt.subplots(figsize=(12, 6))

category_counts = df['category_clean'].value_counts()

bars = ax.bar(range(len(category_counts)), category_counts.values, color=colors)
ax.set_xticks(range(len(category_counts)))
ax.set_xticklabels(category_counts.index, rotation=45, ha='right')

# Add count labels
for i, (idx, val) in enumerate(category_counts.items()):
    percentage = (val / len(df)) * 100
    ax.text(i, val + 5, f'{val}\n({percentage:.1f}%)', ha='center', fontweight='bold')

ax.set_ylabel('Number of Listings', fontsize=12, fontweight='bold')
ax.set_title('Marketplace Inventory Distribution by Property Type', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '02_category_distribution.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 2 saved")

# ============================================================================
# CHART 3: Price Distribution Analysis
# ============================================================================
print("Generating Chart 3: Price Distribution...")
fig, ax = plt.subplots(figsize=(12, 6))

# Filter outliers for better visualization (remove top 5% and bottom 5%)
price_data = df['price_clean'].dropna()
q05 = price_data.quantile(0.05)
q95 = price_data.quantile(0.95)
price_filtered = price_data[(price_data >= q05) & (price_data <= q95)]

ax.hist(price_filtered, bins=40, color='#2E86AB', edgecolor='black', alpha=0.7)

# Add median line
median_price = price_filtered.median()
ax.axvline(median_price, color='red', linestyle='--', linewidth=2, label=f'Median: {median_price:,.0f} AZN')

ax.set_xlabel('Price (AZN)', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Listings', fontsize=12, fontweight='bold')
ax.set_title('Price Distribution Across All Listings (5th-95th Percentile)', fontsize=14, fontweight='bold', pad=20)
ax.legend(fontsize=11)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '03_price_distribution.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 3 saved")

# ============================================================================
# CHART 4: Top 10 Locations by Listing Volume
# ============================================================================
print("Generating Chart 4: Top Locations...")
fig, ax = plt.subplots(figsize=(12, 6))

city_counts = df['city'].value_counts().head(10)

bars = ax.barh(range(len(city_counts)), city_counts.values, color='#F18F01')
ax.set_yticks(range(len(city_counts)))
ax.set_yticklabels(city_counts.index)

# Add count labels
for i, (idx, val) in enumerate(city_counts.items()):
    percentage = (val / len(df)) * 100
    ax.text(val + 2, i, f'{val} ({percentage:.1f}%)', va='center', fontweight='bold')

ax.set_xlabel('Number of Listings', fontsize=12, fontweight='bold')
ax.set_title('Top 10 Markets by Listing Volume', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '04_top_locations.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 4 saved")

# ============================================================================
# CHART 5: Room Count Distribution (for properties with room data)
# ============================================================================
print("Generating Chart 5: Room Count Distribution...")
fig, ax = plt.subplots(figsize=(12, 6))

room_data = df[df['room_count'].notna()]['room_count'].value_counts().sort_index()

bars = ax.bar(room_data.index.astype(str), room_data.values, color='#6A994E', edgecolor='black')

# Add count labels
for i, (idx, val) in enumerate(room_data.items()):
    ax.text(i, val + 1, f'{val}', ha='center', fontweight='bold')

ax.set_xlabel('Number of Rooms', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Listings', fontsize=12, fontweight='bold')
ax.set_title('Property Size Distribution by Room Count', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '05_room_distribution.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 5 saved")

# ============================================================================
# CHART 6: Average Area by Property Category
# ============================================================================
print("Generating Chart 6: Average Area by Category...")
fig, ax = plt.subplots(figsize=(12, 6))

avg_area = df.groupby('category_clean')['area_clean'].mean().sort_values(ascending=False).dropna()

if len(avg_area) > 0:
    bars = ax.barh(avg_area.index, avg_area.values, color='#A23B72')

    # Add value labels
    for i, (idx, val) in enumerate(avg_area.items()):
        ax.text(val + 20, i, f'{val:.0f} m²', va='center', fontweight='bold')

    ax.set_xlabel('Average Area (m²)', fontsize=12, fontweight='bold')
    ax.set_title('Average Property Size by Category', fontsize=14, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / '06_avg_area_by_category.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Chart 6 saved")
else:
    print("⚠ Chart 6 skipped - insufficient area data")
    plt.close()

# ============================================================================
# CHART 7: Listing Quality - Average Image Count by Category
# ============================================================================
print("Generating Chart 7: Image Count Analysis...")
fig, ax = plt.subplots(figsize=(12, 6))

avg_images = df.groupby('category_clean')['image_count'].mean().sort_values(ascending=False)

bars = ax.bar(range(len(avg_images)), avg_images.values, color='#C73E1D', edgecolor='black')
ax.set_xticks(range(len(avg_images)))
ax.set_xticklabels(avg_images.index, rotation=45, ha='right')

# Add value labels
for i, (idx, val) in enumerate(avg_images.items()):
    ax.text(i, val + 0.1, f'{val:.1f}', ha='center', fontweight='bold')

ax.set_ylabel('Average Number of Images', fontsize=12, fontweight='bold')
ax.set_title('Listing Quality: Average Images per Property Category', fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig(CHARTS_DIR / '07_avg_images_by_category.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 7 saved")

# ============================================================================
# CHART 8: Listing Activity Over Time
# ============================================================================
print("Generating Chart 8: Listing Activity Timeline...")
fig, ax = plt.subplots(figsize=(12, 6))

date_counts = df[df['date_posted_clean'].notna()]['date_posted_clean'].value_counts().sort_index()

if len(date_counts) > 0:
    ax.plot(date_counts.index, date_counts.values, marker='o', linewidth=2, color='#2E86AB', markersize=6)
    ax.fill_between(date_counts.index, date_counts.values, alpha=0.3, color='#2E86AB')

    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Listings Posted', fontsize=12, fontweight='bold')
    ax.set_title('Daily Listing Activity on Platform', fontsize=14, fontweight='bold', pad=20)
    ax.grid(alpha=0.3)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / '08_listing_activity_timeline.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Chart 8 saved")
else:
    print("⚠ Chart 8 skipped - insufficient date data")
    plt.close()

# ============================================================================
# CHART 9: Price Range Analysis by Category (Min, Avg, Max)
# ============================================================================
print("Generating Chart 9: Price Range by Category...")
fig, ax = plt.subplots(figsize=(14, 7))

price_stats = df.groupby('category_clean')['price_clean'].agg(['min', 'mean', 'max']).sort_values('mean', ascending=False)

x = np.arange(len(price_stats))
width = 0.25

bars1 = ax.bar(x - width, price_stats['min'], width, label='Minimum', color='#90BE6D', edgecolor='black')
bars2 = ax.bar(x, price_stats['mean'], width, label='Average', color='#F18F01', edgecolor='black')
bars3 = ax.bar(x + width, price_stats['max'], width, label='Maximum', color='#C73E1D', edgecolor='black')

ax.set_xticks(x)
ax.set_xticklabels(price_stats.index, rotation=45, ha='right')
ax.set_ylabel('Price (AZN)', fontsize=12, fontweight='bold')
ax.set_title('Price Range Analysis by Property Category', fontsize=14, fontweight='bold', pad=20)
ax.legend(fontsize=11)
ax.grid(axis='y', alpha=0.3)

# Format y-axis with thousands separator
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000:.0f}K'))

plt.tight_layout()
plt.savefig(CHARTS_DIR / '09_price_range_by_category.png', dpi=300, bbox_inches='tight')
plt.close()
print("✓ Chart 9 saved")

# ============================================================================
# CHART 10: Top Sellers by Listing Volume
# ============================================================================
print("Generating Chart 10: Top Sellers...")
fig, ax = plt.subplots(figsize=(12, 6))

top_sellers = df[df['seller_name'].notna()]['seller_name'].value_counts().head(10)

if len(top_sellers) > 0:
    bars = ax.barh(range(len(top_sellers)), top_sellers.values, color='#277DA1')
    ax.set_yticks(range(len(top_sellers)))
    ax.set_yticklabels(top_sellers.index)

    # Add count labels
    for i, (idx, val) in enumerate(top_sellers.items()):
        ax.text(val + 0.5, i, f'{val}', va='center', fontweight='bold')

    ax.set_xlabel('Number of Active Listings', fontsize=12, fontweight='bold')
    ax.set_title('Top 10 Most Active Sellers on Platform', fontsize=14, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / '10_top_sellers.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Chart 10 saved")
else:
    print("⚠ Chart 10 skipped - insufficient seller data")
    plt.close()

# ============================================================================
# CHART 11: Price vs Area Correlation (for apartments)
# ============================================================================
print("Generating Chart 11: Price vs Area Analysis...")
fig, ax = plt.subplots(figsize=(12, 6))

apartments = df[(df['category_clean'] == 'Apartments') &
                (df['price_clean'].notna()) &
                (df['area_clean'].notna()) &
                (df['area_clean'] < 300) &  # Remove outliers
                (df['price_clean'] < 500000)]  # Remove outliers

if len(apartments) > 10:
    ax.scatter(apartments['area_clean'], apartments['price_clean'],
               alpha=0.5, s=50, color='#2E86AB', edgecolors='black', linewidth=0.5)

    # Add trend line
    z = np.polyfit(apartments['area_clean'], apartments['price_clean'], 1)
    p = np.poly1d(z)
    ax.plot(apartments['area_clean'].sort_values(),
            p(apartments['area_clean'].sort_values()),
            "r--", linewidth=2, label=f'Trend: {z[0]:.0f} AZN/m²')

    ax.set_xlabel('Area (m²)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Price (AZN)', fontsize=12, fontweight='bold')
    ax.set_title('Apartment Pricing: Price vs Area Relationship', fontsize=14, fontweight='bold', pad=20)
    ax.legend(fontsize=11)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / '11_price_vs_area_apartments.png', dpi=300, bbox_inches='tight')
    plt.close()
    print("✓ Chart 11 saved")
else:
    print("⚠ Chart 11 skipped - insufficient apartment data")
    plt.close()

# ============================================================================
# Generate Summary Statistics
# ============================================================================
print("\n" + "="*80)
print("SUMMARY STATISTICS FOR BUSINESS INSIGHTS")
print("="*80)

print(f"\nTotal Listings: {len(df):,}")
print(f"Date Range: {df['date_posted_clean'].min().strftime('%d %B %Y')} to {df['date_posted_clean'].max().strftime('%d %B %Y')}")
print(f"\nPrice Statistics (AZN):")
print(f"  Average: {df['price_clean'].mean():,.0f}")
print(f"  Median: {df['price_clean'].median():,.0f}")
print(f"  Min: {df['price_clean'].min():,.0f}")
print(f"  Max: {df['price_clean'].max():,.0f}")

print(f"\nCategory Breakdown:")
for cat, count in df['category_clean'].value_counts().items():
    pct = (count/len(df))*100
    print(f"  {cat}: {count} ({pct:.1f}%)")

print(f"\nTop 3 Cities:")
for city, count in df['city'].value_counts().head(3).items():
    pct = (count/len(df))*100
    print(f"  {city}: {count} ({pct:.1f}%)")

print(f"\nListing Quality Metrics:")
print(f"  Average Images per Listing: {df['image_count'].mean():.1f}")
print(f"  Listings with Complete Area Data: {df['area_clean'].notna().sum()} ({(df['area_clean'].notna().sum()/len(df)*100):.1f}%)")
print(f"  Listings with Room Count: {df['room_count'].notna().sum()} ({(df['room_count'].notna().sum()/len(df)*100):.1f}%)")

print("\n" + "="*80)
print("All charts generated successfully in 'charts/' directory!")
print("="*80)
