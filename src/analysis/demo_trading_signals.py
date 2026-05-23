#!/usr/bin/env python
"""
Demo: Show the new trading signal visualization
Displays spread analysis with LONG/SHORT/NEUTRAL zones
"""

import sys
sys.path.insert(0, '..')

from plotting.prediction_view import fetch_predictions_df, plot_predictions_df
import matplotlib.pyplot as plt

# Fetch last 240 minutes of predictions (with spread and signal)
print("Fetching predictions with spread analysis...")
df = fetch_predictions_df(lookback_minutes=240, print_status=True)

if df.empty:
    print("No data available")
else:
    print(f"\nData loaded: {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    
    # Show signal distribution
    if 'signal' in df.columns:
        print("\nSignal Distribution:")
        print(df['signal'].value_counts())
    
    # Show spread statistics
    if 'spread' in df.columns:
        print("\nSpread Statistics:")
        print(f"  Min:    {df['spread'].min():.6f}")
        print(f"  Max:    {df['spread'].max():.6f}")
        print(f"  Mean:   {df['spread'].mean():.6f}")
        print(f"  Median: {df['spread'].median():.6f}")
    
    # Plot
    print("\nGenerating plot...")
    fig = plot_predictions_df(df)
    plt.show()

