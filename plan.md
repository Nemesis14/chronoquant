# Feature Engineering Plan for ChronoQuant

## Executive Summary

This plan outlines a systematic approach to **expand from 8 current features to 40+ advanced features**, enabling model performance improvements through:
- Enhanced technical analysis depth
- Multi-timeframe pattern recognition  
- Volume-based signals
- Price action derivatives
- Market microstructure metrics

---

## Part 1: Current Variables Analysis

### Existing Features (8 total)

| Category | Feature | Type | Window(s) |
|----------|---------|------|-----------|
| **Momentum** | RSI | Oscillator | 14 |
| | ROC | Rate of Change | 14, 140 |
| **Trend** | MACD Diff | Trend | 12/26 |
| | SMA Ratio | Ratio | 14, 140 |
| **Volatility** | BB Width | Normalized Width | 14, 140 |

**Current Performance:**
- LONG model: 16.15% accuracy, 1.61x LIFT
- SHORT model: 13.04% accuracy, 1.30x LIFT
- Total features in use: 7-8

**Limitations:**
- Limited momentum indicators (only RSI, ROC)
- Single trend line (SMA, no EMA/WMA)
- No volume analysis
- No price action patterns
- No support/resistance dynamics
- No market microstructure metrics
- No volatility cone or ATR bands
- No correlation or divergence metrics

---

## Part 2: Proposed Extended Feature Set

### Category A: Momentum Indicators (6 new)

| # | Feature | Formula | Window | Logic |
|---|---------|---------|--------|-------|
| A1 | STOCHASTIC_K | %K line | 14 | Overbought/oversold zones |
| A2 | STOCHASTIC_D | %D (SMA of %K) | 3 | Momentum smoothing |
| A3 | CCI | Commodity Channel Index | 20 | Cyclical momentum |
| A4 | WILLIAMS_R | Williams %R | 14 | Inverse RSI logic |
| A5 | ADX | Average Directional Index | 14 | Trend strength |
| A6 | AROON_UP/DN | Aroon oscillator | 25 | Directional trend change |

**Implementation:** Use `ta.momentum.*` from ta-lib

---

### Category B: Trend Indicators (8 new)

| # | Feature | Formula | Window | Logic |
|---|---------|---------|--------|-------|
| B1 | EMA_RATIO | Close / EMA | 14 | Exponential trend |
| B2 | EMA_RATIO | Close / EMA | 140 | Long-term trend |
| B3 | WMA_RATIO | Close / WMA | 14 | Weighted trend |
| B4 | KAMA | Kaufman's Adaptive | 10/2/30 | Adaptive smoothing |
| B5 | MACD_SIGNAL | MACD Signal line | 9 | MACD signal |
| B6 | MACD_HIST | MACD Histogram | - | Momentum histogram |
| B7 | SUPERTREND | Supertrend upper/lower | 10/3 | Support/resistance |
| B8 | VORTEX | VI+ and VI- | 14 | Direction intensity |

**Implementation:** Use `ta.trend.*` or custom calculations

---

### Category C: Volatility Indicators (6 new)

| # | Feature | Formula | Window | Logic |
|---|---------|---------|--------|-------|
| C1 | ATR | Average True Range | 14 | Volatility measure |
| C2 | ATR_RATIO | Close / ATR | 14 | Volatility-adjusted price |
| C3 | NATR | Normalized ATR (%) | 14 | %age volatility |
| C4 | BB_POSITION | (Close - Lower) / (Upper - Lower) | 14 | Band squeeze/expansion |
| C5 | KELTNER_WIDTH | Keltner width | 20/2 | Alternative bands |
| C6 | HISTORICAL_VOL | Std dev of returns | 20 | Rolling volatility |

**Implementation:** Use `ta.volatility.*`

---

### Category D: Volume Analysis (8 new)

| # | Feature | Formula | Window | Logic |
|---|---------|---------|--------|-------|
| D1 | VOLUME_SMA | SMA of volume | 14 | Volume trend |
| D2 | VOLUME_RATIO | Current Vol / Avg Vol | 14 | Volume spike detection |
| D3 | OBV | On-Balance Volume | - | Cumulative volume |
| D4 | OBVCUM_ROC | ROC of OBV | 14 | Volume momentum |
| D5 | MFI | Money Flow Index | 14 | Volume-weighted RSI |
| D6 | AD_LINE | Accumulation/Distribution | - | Money flow |
| D7 | CMF | Chaikin Money Flow | 20 | Money flow ratio |
| D8 | NVT_RATIO | Net Volume Trend | 20 | Close/Volume ratio |

**Implementation:** Use `ta.volume.*`

---

### Category E: Price Action & Derivatives (8 new)

| # | Feature | Formula | Window | Logic |
|---|---------|---------|--------|-------|
| E1 | RETURNS | Log returns | - | Price change % |
| E2 | RETURNS_SMA | SMA of returns | 14 | Trend direction |
| E3 | RETURNS_STD | Std of returns | 14 | Volatility cone |
| E4 | SKEWNESS | Return skewness | 14 | Distribution tail risk |
| E5 | KURTOSIS | Return kurtosis | 14 | Tail events probability |
| E6 | HML_RANGE | (High-Low)/Close | - | Intra-bar range |
| E7 | OHLC_RANGE | (High-Low)/(Open+Close) | - | Volatility measure |
| E8 | CLOSE_POSITION | (Close-Low)/(High-Low) | - | Close within bar |

**Implementation:** Direct pandas/numpy calculations

---

### Category F: Market Structure (6 new)

| # | Feature | Formula | Window | Logic |
|---|---------|---------|--------|-------|
| F1 | HIGHER_HIGH | Count HH in N bars | 5 | Uptrend strength |
| F2 | HIGHER_LOW | Count HL in N bars | 5 | Uptrend integrity |
| F3 | LOWER_HIGH | Count LH in N bars | 5 | Downtrend strength |
| F4 | LOWER_LOW | Count LL in N bars | 5 | Downtrend integrity |
| F5 | SWING_HIGH | Peak detection | 5 | Resistance levels |
| F6 | SWING_LOW | Trough detection | 5 | Support levels |

**Implementation:** Custom rolling logic with argmax/argmin

---

### Category G: Multi-Timeframe Cross-Features (4 new)

| # | Feature | Formula | Window | Logic |
|---|---------|---------|--------|-------|
| G1 | MTF_TREND | 1m vs 5m vs 15m | - | Alignment strength |
| G2 | MTF_DIVERG | Divergence (1m vs 5m RSI) | - | Potential reversal |
| G3 | MICRO_MACRO | 14p vs 140p ratio gap | - | Timeframe spread |
| G4 | MOMENTUM_ACCELERATION | 2nd derivative of momentum | 14 | Acceleration measure |

**Implementation:** Requires 5m/15m data (future enhancement)

---

## Part 3: Feature Implementation Strategy

### Phase 1: Core Expansion (8 features → 20 features)
**Timeline: Week 1-2**

1. Add simple volume metrics (D1, D2, D3)
2. Add price derivatives (E1, E2, E3, E6)
3. Add additional momentum (A1, A5)
4. Total: +12 features (20 total)

### Phase 2: Advanced Indicators (20 → 35 features)
**Timeline: Week 3-4**

5. Add remaining volatility (C1, C2, C3)
6. Add volume money flow (D4, D5, D6, D7)
7. Add trend EMA/WMA (B1, B2, B3)
8. Add market structure (F1, F2)
9. Total: +15 features (35 total)

### Phase 3: Optimization & Selection (35 → 15-20 features)
**Timeline: Week 5-6**

10. Feature correlation analysis (remove redundant)
11. Statistical significance testing (p-values)
12. VIF (Variance Inflation Factor) for multicollinearity
13. Model performance backtest with different subsets
14. Select top 15-20 uncorrelated features

---

## Part 4: Database Implementation

### 4.1 Schema Update

**Current FEATURES table:**
```sql
CREATE TABLE FEATURES (
    open_time TEXT,
    close REAL,
    trg_l_rw_240_prc_09 INTEGER,
    trg_s_rw_240_prc_01 INTEGER,
    feat_rsi_14 REAL,
    feat_roc_14 REAL,
    feat_roc_140 REAL,
    feat_macd_diff REAL,
    feat_sma_ratio_14 REAL,
    feat_sma_ratio_140 REAL,
    feat_bb_width_14 REAL,
    feat_bb_width_140 REAL
)
```

**Proposed additions (Phase 1):**
```python
new_features_phase1 = {
    "feat_volume_sma_14": REAL,
    "feat_volume_ratio_14": REAL,
    "feat_obv": REAL,
    "feat_obv_roc_14": REAL,
    "feat_returns_log": REAL,
    "feat_returns_sma_14": REAL,
    "feat_returns_std_14": REAL,
    "feat_hml_range": REAL,
    "feat_stoch_k_14": REAL,
    "feat_adx_14": REAL,
    "feat_micro_macro_ratio": REAL,
    "feat_momentum_acc_14": REAL
}
```

### 4.2 Feature Config Update

**Update config/features.json:**
```json
{
  "database": {
    "features": {
      "targets": [...],  // unchanged
      "indicators": {
        "momentum": {
          "rsi": [{"window": 14}],
          "roc": [{"window": 14}, {"window": 140}],
          "stochastic": [{"window": 14, "smooth_k": 3, "smooth_d": 3}],
          "adx": [{"window": 14}],
          "aroon": [{"window": 25}]
        },
        "trend": {
          "ema": [{"window": 14}, {"window": 140}],
          "wma": [{"window": 14}],
          "kama": [{"window": 10, "fast": 2, "slow": 30}],
          "macd": [{"fast": 12, "slow": 26, "signal": 9}],
          "supertrend": [{"period": 10, "multiplier": 3}]
        },
        "volatility": {
          "atr": [{"window": 14}],
          "natr": [{"window": 14}],
          "bollinger": [{"window": 14}, {"window": 140}],
          "keltner": [{"window": 20, "multiplier": 2}]
        },
        "volume": {
          "obv": [{}],
          "mfi": [{"window": 14}],
          "cmf": [{"window": 20}],
          "ad_line": [{}]
        },
        "price_action": {
          "returns": [{"type": "log"}],
          "volatility_cone": [{"window": 14, "lookback": 20}],
          "range_metrics": [{}]
        }
      }
    }
  }
}
```

---

## Part 5: Implementation Steps

### Step 1: Extend features.py

```python
# In sync_features(start_time, lookback_bars)

# ===== New: Volume Analysis =====
df["feat_volume_sma_14"] = df["volume"].rolling(14).mean()
df["feat_volume_ratio_14"] = df["volume"] / df["volume"].rolling(14).mean()
df["feat_obv"] = ta.volume.OnBalanceVolumeIndicator(
    close=df["close"], volume=df["volume"]
).on_balance_volume()

# ===== New: Price Returns =====
df["feat_returns_log"] = np.log(df["close"] / df["close"].shift(1))
df["feat_returns_sma_14"] = df["feat_returns_log"].rolling(14).mean()
df["feat_returns_std_14"] = df["feat_returns_log"].rolling(14).std()

# ===== New: Market Structure =====
df["feat_hml_range"] = (df["high"] - df["low"]) / df["close"]
df["feat_close_position"] = (df["close"] - df["low"]) / (df["high"] - df["low"])

# ===== New: Advanced Momentum =====
stoch = ta.momentum.StochasticOscillator(
    high=df["high"], low=df["low"], close=df["close"], window=14
)
df["feat_stoch_k_14"] = stoch.stoch()
df["feat_adx_14"] = ta.trend.ADXIndicator(
    high=df["high"], low=df["low"], close=df["close"], window=14
).adx()
```

### Step 2: Feature Selection & Correlation Analysis

```python
# Create feature_selection.py

import pandas as pd
import numpy as np
from scipy.stats import spearmanr, pointbiserialr
import sqlite3

def select_independent_features(db_path: str, target_col: str) -> list:
	"""
	Select uncorrelated features using:
	1. Correlation matrix (remove >0.7 corr)
	2. VIF (Variance Inflation Factor, <5)
	3. Statistical significance vs target
	4. Information value ranking
	"""
	
	conn = sqlite3.connect(db_path)
	df = pd.read_sql_query(
		f"SELECT * FROM FEATURES WHERE {target_col} IS NOT NULL",
		conn
	)
	conn.close()
	
	# --- Remove NaN ---
	df = df.dropna()
	
	# --- Correlation Matrix ---
	feat_cols = [c for c in df.columns if c.startswith("feat_")]
	corr_matrix = df[feat_cols].corr().abs()
	
	# --- Remove high correlation pairs ---
	upper = corr_matrix.where(
		np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
	)
	high_corr = [col for col in upper.columns if any(upper[col] > 0.7)]
	selected = [f for f in feat_cols if f not in high_corr]
	
	# --- VIF Filter ---
	from statsmodels.stats.outliers_influence import variance_inflation_factor
	
	X = df[selected]
	vif_data = pd.DataFrame()
	vif_data["feature"] = X.columns
	vif_data["VIF"] = [
		variance_inflation_factor(X.values, i) for i in range(X.shape[1])
	]
	
	selected = vif_data[vif_data["VIF"] < 5]["feature"].tolist()
	
	# --- Statistical Significance ---
	p_values = {}
	for feat in selected:
		_, p_val = pointbiserialr(df[target_col], df[feat])
		p_values[feat] = p_val
	
	selected = [f for f in selected if p_values.get(f, 1) < 0.05]
	
	print(f"Selected {len(selected)} independent features (from {len(feat_cols)})")
	print(f"Features: {selected}")
	
	return selected
```

### Step 3: Model Retraining Workflow

```python
# Create model_retrain.py

import json
import pickle
import pandas as pd
import numpy as np
import sqlite3
from statsmodels.formula.api import logit
from sklearn.metrics import classification_report, roc_auc_score

def retrain_model(
	db_path: str,
	model_id: str,
	target_col: str,
	feature_list: list,
	output_dir: str
) -> dict:
	"""
	Retrain model with new features
	
	Parameters:
	  - db_path: Path to SQLite database
	  - model_id: Model identifier (e.g., 'lg_l_rw240_p90_v2')
	  - target_col: Target column name (e.g., 'trg_l_rw_240_prc_09')
	  - feature_list: Selected independent features
	  - output_dir: Directory to save model and metadata
	
	Returns:
	  - dict with: model, features, metrics, p_values
	"""
	
	# --- Load data ---
	conn = sqlite3.connect(db_path)
	df = pd.read_sql_query(
		f"SELECT {','.join([target_col] + feature_list)} FROM FEATURES",
		conn
	)
	conn.close()
	
	df = df.dropna()
	
	# --- Train/Test split (time-based) ---
	split_idx = int(len(df) * 0.8)
	df_train = df.iloc[:split_idx]
	df_test = df.iloc[split_idx:]
	
	# --- Fit logistic regression ---
	formula = f"{target_col} ~ {' + '.join(feature_list)}"
	model = logit(formula, data=df_train).fit()
	
	# --- Predictions ---
	pred_proba = model.predict(df_test[feature_list])
	pred_binary = (pred_proba >= 0.5).astype(int)
	
	# --- Metrics ---
	auc = roc_auc_score(df_test[target_col], pred_proba)
	report = classification_report(
		df_test[target_col], pred_binary, output_dict=True
	)
	
	# --- Save model ---
	os.makedirs(output_dir, exist_ok=True)
	
	with open(f"{output_dir}/model.pkl", "wb") as f:
		pickle.dump(model, f)
	
	with open(f"{output_dir}/features.json", "w") as f:
		json.dump({"features": feature_list}, f, indent=2)
	
	# --- Save metadata ---
	metadata = {
		"model_id": model_id,
		"target": target_col,
		"features": feature_list,
		"train_samples": len(df_train),
		"test_samples": len(df_test),
		"auc_score": auc,
		"accuracy": report["accuracy"],
		"precision": report["1"]["precision"],
		"recall": report["1"]["recall"],
		"f1_score": report["1"]["f1-score"],
		"p_values": model.pvalues.to_dict(),
		"coefficients": model.params.to_dict()
	}
	
	with open(f"{output_dir}/metadata.json", "w") as f:
		json.dump(metadata, f, indent=2)
	
	print(f"✅ Model trained: AUC={auc:.3f}, Accuracy={report['accuracy']:.3f}")
	print(f"Saved to: {output_dir}")
	
	return metadata
```

---

## Part 6: Feature Selection Methodology

### Step 1: Correlation Analysis
```
Input: 35+ features
├─ Compute Pearson/Spearman correlation matrix
├─ Identify feature pairs with |r| > 0.7
├─ Keep one from each pair (highest information value)
└─ Output: ~25-28 features
```

### Step 2: VIF (Multicollinearity) Filter
```
Input: 25-28 features
├─ Calculate Variance Inflation Factor for each
├─ Remove features with VIF > 5 (high multicollinearity)
└─ Output: ~20-22 features
```

### Step 3: Statistical Significance
```
Input: 20-22 features
├─ Test correlation with target (p-value < 0.05)
├─ Remove features not significantly related
└─ Output: ~15-18 features
```

### Step 4: Model Performance Backtest
```
Input: 15-18 features
├─ Train multiple logistic regression models:
│  ├─ All 18 features
│  ├─ Top 15 (by coefficient magnitude)
│  ├─ Top 12 (by information value)
│  └─ Top 10 (by feature importance)
├─ Compare on hold-out test set: AUC, Accuracy, Precision
└─ Output: Final feature set (optimal subset)
```

---

## Part 7: Retraining Workflow

### Workflow Diagram

```
┌─────────────────────────────────────┐
│ 1. Calculate 40+ Extended Features   │  (database_codes/features.py)
│    Store in FEATURES table           │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│ 2. Feature Selection                 │  (database_codes/feature_selection.py)
│    ├─ Correlation filter (r<0.7)     │
│    ├─ VIF filter (VIF<5)             │
│    ├─ P-value filter (p<0.05)        │
│    └─ Output: 15-18 features         │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│ 3. Model Retraining                  │  (database_codes/model_retrain.py)
│    ├─ Train logistic regression      │
│    ├─ Evaluate on test set           │
│    └─ Save model + features.json     │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│ 4. Register New Model                │
│    ├─ Update config/models.json      │
│    ├─ Set active: true               │
│    └─ predictions.py reads it        │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│ 5. Live Predictions                  │
│    ├─ worker.py calls sync_features  │
│    ├─ Computes 40+ features          │
│    ├─ sync_predictions reads model   │
│    └─ Generate signals               │
└─────────────────────────────────────┘
```

---

## Part 8: Configuration Updates

### Update config/features.json

Add new feature definitions:

```json
{
  "database": {
    "features": {
      "targets": [
        {"direction": "long", "name": "trg_l_rw_240_prc_09", "rolling_window": 240, "percentile": 0.9},
        {"direction": "short", "name": "trg_s_rw_240_prc_01", "rolling_window": 240, "percentile": 0.1}
      ],
      "indicators": {
        "momentum": {
          "rsi": [{"window": 14}],
          "roc": [{"window": 14}, {"window": 140}],
          "stochastic": [{"window": 14, "smooth_k": 3, "smooth_d": 3}],
          "cci": [{"window": 20}],
          "williams_r": [{"window": 14}],
          "adx": [{"window": 14}]
        },
        "trend": {
          "sma": [{"window": 14}, {"window": 140}],
          "ema": [{"window": 14}, {"window": 140}],
          "wma": [{"window": 14}],
          "kama": [{"window": 10, "fast": 2, "slow": 30}],
          "macd": [{"fast": 12, "slow": 26, "signal": 9}]
        },
        "volatility": {
          "bollinger": [{"window": 14}, {"window": 140}],
          "atr": [{"window": 14}],
          "natr": [{"window": 14}],
          "keltner": [{"window": 20, "multiplier": 2}]
        },
        "volume": {
          "obv": [{}],
          "obv_roc": [{"window": 14}],
          "mfi": [{"window": 14}],
          "ad_line": [{}],
          "cmf": [{"window": 20}],
          "volume_ratio": [{"window": 14}]
        },
        "price_action": {
          "returns": [{"type": "log"}],
          "returns_sma": [{"window": 14}],
          "returns_std": [{"window": 14}],
          "range_metrics": [{}],
          "close_position": [{}]
        },
        "market_structure": {
          "swing_points": [{"lookback": 5}],
          "trend_lines": [{"periods": 5}]
        }
      }
    }
  }
}
```

### Update config/models.json

Register new model:

```json
{
  "models": {
    "lg_l_rw240_p90_base_sm": {
      "target_name": "trg_l_rw_240_prc_09",
      "active": false,
      "paths": {"model_dir": "model_dev/lg_l_rw240_p90_base_sm", "model_file": "model.pkl", "features_file": "features.json"}
    },
    "lg_l_rw240_p90_v2_extended": {
      "target_name": "trg_l_rw_240_prc_09",
      "active": true,
      "paths": {"model_dir": "model_dev/lg_l_rw240_p90_v2_extended", "model_file": "model.pkl", "features_file": "features.json"},
      "description": "Extended features: 35+ indicators, selected 18 features"
    },
    "lg_s_rw240_p90_base_sm": {
      "target_name": "trg_s_rw_240_prc_01",
      "active": false,
      "paths": {"model_dir": "model_dev/lg_s_rw240_p90_base_sm", "model_file": "model.pkl", "features_file": "features.json"}
    },
    "lg_s_rw240_p90_v2_extended": {
      "target_name": "trg_s_rw_240_prc_01",
      "active": true,
      "paths": {"model_dir": "model_dev/lg_s_rw240_p90_v2_extended", "model_file": "model.pkl", "features_file": "features.json"},
      "description": "Extended features: 35+ indicators, selected 18 features"
    }
  }
}
```

---

## Part 9: Implementation Timeline

### Week 1: Foundation
- [ ] Update features.json with new indicator definitions
- [ ] Implement Phase 1 features in features.py (Volume + Price Action)
- [ ] Test database insert with new columns
- [ ] Backfill historical data with new features

### Week 2: Feature Expansion
- [ ] Implement Phase 2 features (Volatility, Trend, Momentum)
- [ ] Compute all 35+ features for historical data
- [ ] Create feature_selection.py
- [ ] Run correlation and VIF analysis

### Week 3: Selection & Validation
- [ ] Apply statistical significance filtering
- [ ] Generate correlation matrix heatmap
- [ ] Document selected features and rationale
- [ ] Create feature importance ranking

### Week 4: Model Retraining
- [ ] Implement model_retrain.py
- [ ] Train new models with selected features
- [ ] Backtest on historical data (compare vs baseline)
- [ ] Save models to model_dev/lg_*_v2_extended/

### Week 5: Deployment
- [ ] Update config/models.json
- [ ] Set new models as active
- [ ] Run live predictions on live data
- [ ] Monitor signal quality and performance
- [ ] A/B test: old vs new model signals

### Week 6: Optimization
- [ ] Collect performance metrics
- [ ] Fine-tune feature weights
- [ ] Adjust model hyperparameters
- [ ] Document results and learnings

---

## Part 10: Performance Expectations

### Conservative Estimate
- Baseline (current): 16.15% LONG accuracy, 1.61x LIFT
- Target: **18-20% LONG accuracy** (1.8x-2.0x LIFT)
- Target: **15-17% SHORT accuracy** (1.5x-1.8x LIFT)

### Success Metrics
| Metric | Baseline | Target | Improvement |
|--------|----------|--------|-------------|
| LONG Accuracy | 16.15% | 19% | +2.85 pp |
| LONG LIFT | 1.61x | 1.95x | +0.34x |
| SHORT Accuracy | 13.04% | 16% | +2.96 pp |
| SHORT LIFT | 1.30x | 1.65x | +0.35x |
| Model Size | 7-8 feat | 15-18 feat | 2x expansion |
| Signal Frequency | ~10% events | ~10% events | Unchanged |

---

## Part 11: Risk Mitigation

### Overfitting Prevention
- [ ] Use time-based train/test split (80/20)
- [ ] Cross-validate on sliding windows
- [ ] Regularization (L1/L2) in logistic regression
- [ ] Monitor test set performance vs training

### Data Quality
- [ ] Check for NaN/missing values before model training
- [ ] Validate feature ranges (outlier detection)
- [ ] Compare raw OHLCV with Binance API

### Feature Drift
- [ ] Monitor feature distributions over time
- [ ] Alert if feature values deviate >3 std dev
- [ ] Retrain model quarterly or after market regime change

### Model Degradation
- [ ] Track signal accuracy in real-time
- [ ] Compare old vs new model predictions
- [ ] Fallback to baseline if new model underperforms

---

## Part 12: Quick Reference

### Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| database_codes/features.py | EXTEND | Add 35+ new indicators |
| config/features.json | UPDATE | Define new features |
| config/models.json | UPDATE | Register v2 models |
| database_codes/feature_selection.py | CREATE | Feature filtering logic |
| database_codes/model_retrain.py | CREATE | Model training pipeline |
| model_dev/lg_l_rw240_p90_v2_extended/ | CREATE | New model directory |
| model_dev/lg_s_rw240_p90_v2_extended/ | CREATE | New model directory |
| analysis/feature_analysis.ipynb | CREATE | Jupyter analysis notebook |

### Key Python Libraries
```python
import ta  # Technical Analysis
import statsmodels  # Logistic Regression
import sklearn  # Metrics, model evaluation, Lasso
import numpy as pd  # Numerical computing
import pandas as pd  # Data manipulation
from sklearn.linear_model import LogisticRegression  # L1 (Lasso)
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score, StratifiedKFold
```

---

## Part 13: Lasso Regression Feature Selection Strategy

### 13.1 Potential Variable Count Analysis

#### Current Approach (Manual Selection)
- **Base features:** 40+ indicators across 7 categories
- **Time windows:** 14, 20, 25, 140 periods (primary)
- **Secondary windows:** 5, 10, 50, 200 periods (alternative)
- **Total potential variants:** 40+ indicators × 4-5 time windows = **160-200+ features**

#### If We Calculate ALL Potential Variables
Expanding the feature space comprehensively:

| Category | Base Features | Time Windows | Total Variants |
|----------|--------------|--------------|-----------------|
| **Momentum** (6) | RSI, ROC, Stoch, CCI, Williams, ADX | 14, 140 | 12 |
| **Trend** (8) | SMA, EMA, WMA, KAMA, MACD, Signal, Hist, Vortex | 14, 140, 5, 50 | 24 |
| **Volatility** (6) | ATR, NATR, BB Width, Keltner, HistVol, Range | 14, 140, 20 | 18 |
| **Volume** (8) | OBV, MFI, AD Line, CMF, Vol SMA, Vol Ratio, NVT, Vol Momentum | 14, 20, 140 | 24 |
| **Price Action** (8) | Returns, Returns SMA, Returns Std, Skew, Kurtosis, HML, OHLC, Close Pos | 14, 140, 5 | 24 |
| **Market Structure** (6) | HH, HL, LH, LL, Swing High, Swing Low | 5, 10, 20 | 18 |
| **Derivatives** (4) | Ratios, Spreads, Divergences, Acceleration | Multi-window | 16 |
| **Cross-Features** (4) | MTF Trend, MTF Diverg, Micro-Macro, Momentum Accel | - | 8 |
| | | **TOTAL POTENTIAL** | **~144 features** |

#### Adding More Time Windows (Conservative)
If we use {5, 10, 14, 20, 25, 50, 140, 200} = 8 windows:
- **Potential features: 200-250+**

#### With Rolling Derivatives (Aggressive)
If we add: 1st derivatives, 2nd derivatives, rate-of-change for each feature:
- **Potential features: 400-600+**

### 13.2 Feasibility Analysis: Can Lasso Handle This?

#### Sample Size Assessment

**ChronoQuant Data:**
- Frequency: 1-minute candles
- Daily samples: 1,440 (24h × 60 min)
- Monthly samples: ~43,200
- Yearly samples: ~525,600

**Lasso Feasibility Guidelines:**
| Scenario | Feature Count | Min Samples | Ratio | Stability |
|----------|--------------|------------|--------|-----------|
| **Comfortable** | p < n/10 | n > 10p | >10:1 | ✓✓✓ Stable |
| **Acceptable** | p < n/5 | n > 5p | >5:1 | ✓✓ Good |
| **Tight** | p < n/2 | n > 2p | >2:1 | ✓ Workable |
| **Risky** | p > n/2 | n < 2p | <2:1 | ✗ Unstable |

**For ChronoQuant:**

Scenario A: **144 features, 1 month data (43,200 samples)**
- Ratio: 43,200 / 144 = 300:1 ✓✓✓ **VERY COMFORTABLE**
- Lasso can handle easily

Scenario B: **200 features, 1 week data (10,080 samples)**
- Ratio: 10,080 / 200 = 50:1 ✓✓✓ **VERY STABLE**
- Lasso can handle well

Scenario C: **250 features, 1 day data (1,440 samples)**
- Ratio: 1,440 / 250 = 5.76:1 ✓✓ **ACCEPTABLE**
- Lasso can work with regularization tuning

**Conclusion:** Sample size is **SUFFICIENT** for Lasso even with high feature count. We have abundant data (1-minute candles).

### 13.3 Recommended Lasso Workflow

#### Strategy 1: Full Automatic Lasso (Recommended)

Calculate all 144-200 potential features, then use Lasso to automatically select optimal subset:

```python
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler

def lasso_feature_selection(
    X: pd.DataFrame,
    y: pd.Series,
    n_folds: int = 5,
    alphas: list = None
) -> dict:
    """
    Select features using L1-regularized Logistic Regression (Lasso)
    
    Parameters:
      - X: Feature matrix (n_samples × p_features)
      - y: Binary target
      - n_folds: K-fold cross-validation splits
      - alphas: Regularization strengths to test
    
    Returns:
      - dict with: best_alpha, selected_features, cv_scores, model
    """
    
    if alphas is None:
        alphas = np.logspace(-4, 1, 50)
    
    # ===== Standardize features =====
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    cv = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=42)
    
    best_score = -np.inf
    best_alpha = None
    best_model = None
    results = []
    
    # ===== Test each regularization strength =====
    for alpha in alphas:
        
        # Train on CV folds
        lr = LogisticRegression(
            penalty='l1',
            solver='saga',
            C=1/alpha,  # sklearn uses inverse: C = 1/alpha
            max_iter=1000,
            random_state=42
        )
        
        cv_scores = cross_val_score(
            lr, X_scaled, y, cv=cv, scoring='roc_auc'
        )
        
        mean_score = cv_scores.mean()
        results.append({
            'alpha': alpha,
            'mean_cv_auc': mean_score,
            'std_cv_auc': cv_scores.std(),
            'n_features_selected': np.sum(lr.coef_[0] != 0)
        })
        
        # Track best model
        if mean_score > best_score:
            best_score = mean_score
            best_alpha = alpha
            best_model = lr
    
    # ===== Get selected features =====
    best_model.fit(X_scaled, y)
    feature_mask = best_model.coef_[0] != 0
    selected_features = X.columns[feature_mask].tolist()
    
    print(f"✓ Lasso Selection Complete")
    print(f"  Alpha: {best_alpha:.6f}")
    print(f"  CV AUC: {best_score:.4f} ± {cv_scores.std():.4f}")
    print(f"  Features selected: {len(selected_features)} / {X.shape[1]}")
    print(f"  Compression ratio: {X.shape[1] / len(selected_features):.1f}x")
    
    return {
        'best_alpha': best_alpha,
        'best_score': best_score,
        'selected_features': selected_features,
        'feature_coefficients': dict(zip(X.columns, best_model.coef_[0])),
        'scaler': scaler,
        'cv_results': pd.DataFrame(results),
        'model': best_model
    }
```

#### Strategy 2: Group-based Pre-filtering (Optional)

If raw Lasso seems unstable, pre-filter by category before Lasso:

```python
def lasso_with_preprocessing(X: pd.DataFrame, y: pd.Series) -> dict:
    """
    Pre-filter features by category, then apply Lasso within groups
    """
    
    categories = {
        'momentum': [c for c in X.columns if 'rsi' in c or 'roc' in c or 'stoch' in c],
        'trend': [c for c in X.columns if 'sma' in c or 'ema' in c or 'macd' in c],
        'volatility': [c for c in X.columns if 'atr' in c or 'bb' in c],
        'volume': [c for c in X.columns if 'obv' in c or 'mfi' in c],
        'price_action': [c for c in X.columns if 'returns' in c or 'range' in c],
    }
    
    selected_by_group = {}
    
    # Lasso within each group
    for group, features in categories.items():
        if len(features) == 0:
            continue
        
        X_group = X[features]
        result = lasso_feature_selection(X_group, y, n_folds=5)
        
        selected_by_group[group] = {
            'selected': result['selected_features'],
            'count': len(result['selected_features']),
            'cv_auc': result['best_score']
        }
    
    # Combine across groups
    all_selected = []
    for group, data in selected_by_group.items():
        all_selected.extend(data['selected'])
    
    print(f"\n✓ Group-based Pre-filtering Complete:")
    for group, data in selected_by_group.items():
        print(f"  {group}: {data['count']} features selected (AUC: {data['cv_auc']:.4f})")
    
    return {
        'by_group': selected_by_group,
        'combined_features': all_selected,
        'total_selected': len(all_selected)
    }
```

### 13.4 Cross-Validation Strategy

**5-Fold Stratified Cross-Validation:**

```
Original Data (1 month, ~43,200 samples)
│
├─ FOLD 1: Train on folds 1-4 (80%), Validate on fold 5 (20%)
├─ FOLD 2: Train on folds 1,3,4,5 (80%), Validate on fold 2 (20%)
├─ FOLD 3: Train on folds 1,2,4,5 (80%), Validate on fold 3 (20%)
├─ FOLD 4: Train on folds 1,2,3,5 (80%), Validate on fold 4 (20%)
└─ FOLD 5: Train on folds 1,2,3,4 (80%), Validate on fold 5 (20%)

Final Model: Train Lasso on ALL data using best alpha from CV
Test Evaluation: Independent hold-out test set (last 1 week of data)
```

**Nested CV for Hyperparameter Tuning:**

```
Outer Loop (5 folds): Evaluate final model performance
  │
  └─ Inner Loop (5 folds): Tune alpha parameter
        │
        └─ Test alpha values: 10^-4, 10^-3, ..., 10^1
```

### 13.5 Decision: Full Auto-Lasso (Recommended)

**Recommendation:** YES, calculate all 144-200 potential variables and use automatic Lasso selection.

**Rationale:**
1. ✓ **Sample size is EXCELLENT** (43,200+ samples for 200 features = 216:1 ratio)
2. ✓ **Lasso is robust** to high-dimensional data with proper regularization
3. ✓ **No pre-filtering needed** — sample size handles the full feature space
4. ✓ **Fully automated** — no manual correlation/VIF filtering required
5. ✓ **Optimal results** — Lasso finds globally best feature subset via CV
6. ✓ **Time efficient** — Faster than manual filtering workflow
7. ✓ **Reproducible** — Cross-validation ensures stable, generalizable features

**Alternative (if concerned about stability):**
- Use **group-based pre-filtering** first (momentum, trend, volume, etc.)
- Then apply Lasso within each group
- Combine selected features across groups
- This adds interpretability but may miss cross-category synergies

**Revised Timeline:**

| Phase | Timeline | Action |
|-------|----------|--------|
| **1** | Week 1 | Compute all 144-200 potential features in database |
| **2** | Week 2 | Implement Lasso CV workflow in Python |
| **3** | Week 3 | Run Lasso selection: tune alpha, select features |
| **4** | Week 4 | Train final model with selected features |
| **5** | Week 5 | Backtest & validate on hold-out test set |
| **6** | Week 6 | Deploy & monitor live performance |

---

## Summary

This plan provides a **structured, phased approach** to:

1. **Expand features** from 8 → 35+ indicators across 7 categories
2. **Calculate potential feature space** (144-200+ variables with time windows)
3. **Apply Lasso regression** for automatic, optimal variable selection via cross-validation
4. **No manual pre-filtering needed** — sample size (43,200+) is excellent for Lasso
5. **Train final model** with selected features
6. **Validate improvements** through backtesting and live monitoring
7. **Deploy confidently** with fallback strategies

**Expected improvement:** **+3-5% accuracy, +0.4-0.6x LIFT** for both LONG and SHORT signals (better than manual selection due to Lasso's optimization).

