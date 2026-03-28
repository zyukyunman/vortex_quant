#!/usr/bin/env python3
"""Quick data coverage check"""
import os
import pandas as pd

DATA = "/Users/zhuquanmin/Documents/plan/data"

print("=== Fina Indicator ===")
df = pd.read_parquet(f'{DATA}/fundamental/fina_indicator.parquet')
print(f"Rows: {len(df)}, end_date: {sorted(df['end_date'].unique())}")

print("\n=== Cashflow ===")
df = pd.read_parquet(f'{DATA}/fundamental/cashflow.parquet')
print(f"Rows: {len(df)}, end_date: {sorted(df['end_date'].unique())}")

print("\n=== Balancesheet ===")
df = pd.read_parquet(f'{DATA}/fundamental/balancesheet.parquet')
print(f"Rows: {len(df)}, end_date: {sorted(df['end_date'].unique())}")

print("\n=== Income ===")
df = pd.read_parquet(f'{DATA}/fundamental/income.parquet')
print(f"Rows: {len(df)}, end_date: {sorted(df['end_date'].unique())}")

print("\n=== Dividend ===")
df = pd.read_parquet(f'{DATA}/fundamental/dividend.parquet')
print(f"Rows: {len(df)}, end_date range: {df['end_date'].min()} ~ {df['end_date'].max()}")

print("\n=== Valuation ===")
for y in range(2014, 2027):
    p = f'{DATA}/fundamental/valuation/{y}.parquet'
    if os.path.exists(p):
        df = pd.read_parquet(p)
        print(f"  {y}: {len(df)} rows")
    else:
        print(f"  {y}: MISSING")

print("\n=== Daily ===")
for y in range(2014, 2027):
    p = f'{DATA}/market/daily/{y}.parquet'
    if os.path.exists(p):
        df = pd.read_parquet(p)
        print(f"  {y}: {len(df)} rows")
    else:
        print(f"  {y}: MISSING")

print("\n=== Index Daily ===")
idx_dir = f'{DATA}/market/index_daily'
if os.path.exists(idx_dir):
    for f in sorted(os.listdir(idx_dir)):
        print(f"  {f}")
else:
    print("  DIRECTORY MISSING")
