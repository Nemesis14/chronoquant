#!/usr/bin/env python
"""
Update predictions table with spread and signal columns.
Implements the cut-off strategy from ANALYSIS.md
"""

import sqlite3
import sys
sys.path.insert(0, '.')
import utils

def main():
    db_cfg = utils.load_db_config()['database']
    db_path = db_cfg['db_path']
    table = db_cfg['tables']['predictions']
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if spread column exists
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'spread' not in columns:
        print('Adding spread column to predictions table...')
        cursor.execute(f'ALTER TABLE {table} ADD COLUMN spread REAL')
        conn.commit()
        
        # Calculate and update spreads
        cursor.execute(f'''
        UPDATE {table}
        SET spread = lg_l_rw240_p90_base_sm_p - lg_s_rw240_p90_base_sm_p
        ''')
        conn.commit()
        print(f'✅ Added and calculated spread for {cursor.rowcount} rows')
    else:
        print('Spread column already exists. Updating values...')
        cursor.execute(f'''
        UPDATE {table}
        SET spread = lg_l_rw240_p90_base_sm_p - lg_s_rw240_p90_base_sm_p
        ''')
        conn.commit()
        print(f'✅ Updated spread for {cursor.rowcount} rows')
    
    # Add signal column for trading logic
    if 'signal' not in columns:
        print('Adding signal column...')
        cursor.execute(f'ALTER TABLE {table} ADD COLUMN signal TEXT')
        conn.commit()
    
    # Calculate signals based on cut-offs from ANALYSIS.md
    # P90 = +0.0139 (LONG), P10 = -0.0142 (SHORT)
    cursor.execute(f'''
    UPDATE {table}
    SET signal = CASE 
        WHEN spread >= 0.0139 THEN 'LONG'
        WHEN spread <= -0.0142 THEN 'SHORT'
        ELSE 'NEUTRAL'
    END
    ''')
    conn.commit()
    print(f'✅ Calculated signals for {cursor.rowcount} rows')
    
    # Verify
    cursor.execute(f'''
    SELECT 
        signal,
        COUNT(*) as cnt,
        ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM {table}), 2) as pct
    FROM {table}
    GROUP BY signal
    ORDER BY cnt DESC
    ''')
    
    print()
    print('Signal Distribution:')
    print('-' * 50)
    for signal, cnt, pct in cursor.fetchall():
        print(f'  {signal:8s}: {cnt:10d} ({pct:6.2f}%)')
    
    conn.close()
    print()
    print('✅ Predictions table updated successfully!')

if __name__ == '__main__':
    main()
