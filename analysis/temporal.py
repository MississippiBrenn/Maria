#!/usr/bin/env python3
"""
Temporal Analysis Module

Analyze time-based patterns in missing persons and unidentified remains data.
Includes trend analysis, seasonality detection, and temporal clustering.
"""

import os
import json
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


class TemporalAnalyzer:
    """
    Analyze temporal patterns in missing persons and unidentified remains cases.
    """

    def __init__(self, mp_df: pd.DataFrame, up_df: pd.DataFrame):
        """
        Initialize with missing persons and unidentified remains DataFrames.

        Args:
            mp_df: DataFrame with 'last_seen_date' column
            up_df: DataFrame with 'found_date' column
        """
        self.mp = mp_df.copy()
        self.up = up_df.copy()

        # Parse dates
        self.mp['date'] = pd.to_datetime(self.mp['last_seen_date'], errors='coerce')
        self.up['date'] = pd.to_datetime(self.up['found_date'], errors='coerce')

        # Extract date components
        for df in [self.mp, self.up]:
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day_of_week'] = df['date'].dt.dayofweek  # 0=Monday
            df['day_of_year'] = df['date'].dt.dayofyear
            df['quarter'] = df['date'].dt.quarter

    def yearly_trends(self) -> pd.DataFrame:
        """
        Analyze year-over-year trends.

        Returns:
            DataFrame with yearly statistics for both MP and UP
        """
        mp_yearly = self.mp.groupby('year').size().reset_index(name='mp_count')
        up_yearly = self.up.groupby('year').size().reset_index(name='up_count')

        # Merge
        yearly = mp_yearly.merge(up_yearly, on='year', how='outer').fillna(0)
        yearly['mp_count'] = yearly['mp_count'].astype(int)
        yearly['up_count'] = yearly['up_count'].astype(int)
        yearly['total'] = yearly['mp_count'] + yearly['up_count']

        # Calculate year-over-year change
        yearly = yearly.sort_values('year')
        yearly['mp_yoy_change'] = yearly['mp_count'].pct_change() * 100
        yearly['up_yoy_change'] = yearly['up_count'].pct_change() * 100

        # Filter to reasonable year range
        yearly = yearly[(yearly['year'] >= 1960) & (yearly['year'] <= 2030)]

        return yearly.reset_index(drop=True)

    def monthly_seasonality(self) -> pd.DataFrame:
        """
        Analyze monthly patterns to detect seasonality.

        Returns:
            DataFrame with monthly statistics
        """
        month_names = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }

        mp_monthly = self.mp.groupby('month').size().reset_index(name='mp_count')
        up_monthly = self.up.groupby('month').size().reset_index(name='up_count')

        monthly = mp_monthly.merge(up_monthly, on='month', how='outer').fillna(0)
        monthly['mp_count'] = monthly['mp_count'].astype(int)
        monthly['up_count'] = monthly['up_count'].astype(int)
        monthly['month_name'] = monthly['month'].map(month_names)

        # Calculate deviation from mean
        mp_mean = monthly['mp_count'].mean()
        up_mean = monthly['up_count'].mean()
        monthly['mp_deviation_pct'] = ((monthly['mp_count'] - mp_mean) / mp_mean * 100).round(1)
        monthly['up_deviation_pct'] = ((monthly['up_count'] - up_mean) / up_mean * 100).round(1)

        return monthly.sort_values('month')

    def day_of_week_patterns(self) -> pd.DataFrame:
        """
        Analyze day-of-week patterns.

        Returns:
            DataFrame with day-of-week statistics
        """
        day_names = {
            0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday',
            4: 'Friday', 5: 'Saturday', 6: 'Sunday'
        }

        mp_dow = self.mp.groupby('day_of_week').size().reset_index(name='mp_count')
        up_dow = self.up.groupby('day_of_week').size().reset_index(name='up_count')

        dow = mp_dow.merge(up_dow, on='day_of_week', how='outer').fillna(0)
        dow['mp_count'] = dow['mp_count'].astype(int)
        dow['up_count'] = dow['up_count'].astype(int)
        dow['day_name'] = dow['day_of_week'].map(day_names)

        return dow.sort_values('day_of_week')

    def era_analysis(self) -> Dict[str, Dict]:
        """
        Analyze cases by historical era.

        Returns:
            Dict with era statistics
        """
        eras = {
            'Pre-1980': (1900, 1979),
            '1980s': (1980, 1989),
            '1990s': (1990, 1999),
            '2000s': (2000, 2009),
            '2010s': (2010, 2019),
            '2020s': (2020, 2029),
        }

        results = {}
        for era_name, (start_year, end_year) in eras.items():
            mp_count = len(self.mp[
                (self.mp['year'] >= start_year) & (self.mp['year'] <= end_year)
            ])
            up_count = len(self.up[
                (self.up['year'] >= start_year) & (self.up['year'] <= end_year)
            ])

            results[era_name] = {
                'mp_count': mp_count,
                'up_count': up_count,
                'total': mp_count + up_count,
                'years': f'{start_year}-{end_year}'
            }

        return results

    def find_temporal_clusters(
        self,
        window_days: int = 30,
        min_cases: int = 5,
        case_type: str = 'mp'
    ) -> List[Dict]:
        """
        Find clusters of cases occurring close together in time.

        Args:
            window_days: Size of sliding window in days
            min_cases: Minimum cases in window to be a cluster
            case_type: 'mp' or 'up'

        Returns:
            List of temporal cluster definitions
        """
        df = self.mp if case_type == 'mp' else self.up
        df = df[df['date'].notna()].copy()
        df = df.sort_values('date')

        clusters = []
        dates = df['date'].values

        i = 0
        while i < len(dates):
            window_start = dates[i]
            window_end = window_start + np.timedelta64(window_days, 'D')

            # Count cases in window
            in_window = df[(df['date'] >= window_start) & (df['date'] < window_end)]

            if len(in_window) >= min_cases:
                # Get state distribution in cluster
                state_counts = in_window['state'].value_counts().head(3).to_dict()

                clusters.append({
                    'start_date': pd.Timestamp(window_start).strftime('%Y-%m-%d'),
                    'end_date': pd.Timestamp(window_end).strftime('%Y-%m-%d'),
                    'case_count': len(in_window),
                    'case_type': case_type.upper(),
                    'top_states': state_counts,
                    'case_ids': in_window['id'].tolist()[:10]  # Sample
                })

                # Skip past this cluster
                i += len(in_window) - 1

            i += 1

        # Sort by case count
        clusters.sort(key=lambda x: x['case_count'], reverse=True)

        return clusters[:50]  # Top 50 clusters

    def gap_analysis(self) -> Dict:
        """
        Analyze the time gap between MP last seen and UP found dates.

        Uses matched pairs from scored matches if available.

        Returns:
            Dict with gap statistics
        """
        # If we have matches, analyze actual gaps
        # For now, analyze theoretical gaps by comparing distributions

        mp_dates = self.mp['date'].dropna()
        up_dates = self.up['date'].dropna()

        if len(mp_dates) == 0 or len(up_dates) == 0:
            return {'error': 'No date data available'}

        return {
            'mp_date_range': {
                'earliest': mp_dates.min().strftime('%Y-%m-%d'),
                'latest': mp_dates.max().strftime('%Y-%m-%d'),
                'median': mp_dates.median().strftime('%Y-%m-%d'),
            },
            'up_date_range': {
                'earliest': up_dates.min().strftime('%Y-%m-%d'),
                'latest': up_dates.max().strftime('%Y-%m-%d'),
                'median': up_dates.median().strftime('%Y-%m-%d'),
            },
            'overlap_years': list(range(
                max(mp_dates.min().year, up_dates.min().year),
                min(mp_dates.max().year, up_dates.max().year) + 1
            ))
        }

    def match_temporal_analysis(self, matches_df: pd.DataFrame) -> Dict:
        """
        Analyze temporal patterns in scored matches.

        Args:
            matches_df: DataFrame with mp_id, up_id, days_gap columns

        Returns:
            Dict with temporal match statistics
        """
        if 'days_gap' not in matches_df.columns:
            return {'error': 'No days_gap column in matches'}

        gaps = matches_df['days_gap'].dropna()

        if len(gaps) == 0:
            return {'error': 'No gap data available'}

        # Gap distribution buckets
        buckets = {
            'within_week': len(gaps[gaps <= 7]),
            'within_month': len(gaps[(gaps > 7) & (gaps <= 30)]),
            'within_6_months': len(gaps[(gaps > 30) & (gaps <= 180)]),
            'within_year': len(gaps[(gaps > 180) & (gaps <= 365)]),
            'within_5_years': len(gaps[(gaps > 365) & (gaps <= 1825)]),
            'over_5_years': len(gaps[gaps > 1825]),
        }

        return {
            'total_with_gap_data': len(gaps),
            'gap_buckets': buckets,
            'statistics': {
                'mean_days': round(gaps.mean(), 1),
                'median_days': round(gaps.median(), 1),
                'std_days': round(gaps.std(), 1),
                'min_days': int(gaps.min()),
                'max_days': int(gaps.max()),
            }
        }

    def export_timeline_data(self, output_path: str) -> str:
        """
        Export data formatted for timeline visualization.

        Args:
            output_path: Path to write JSON file

        Returns:
            Path to created file
        """
        # Aggregate by year and month
        mp_timeline = self.mp.groupby(['year', 'month']).size().reset_index(name='count')
        mp_timeline['type'] = 'MP'

        up_timeline = self.up.groupby(['year', 'month']).size().reset_index(name='count')
        up_timeline['type'] = 'UP'

        timeline = pd.concat([mp_timeline, up_timeline])
        timeline = timeline[
            (timeline['year'] >= 1960) &
            (timeline['year'] <= 2030) &
            (timeline['year'].notna()) &
            (timeline['month'].notna())
        ]

        # Convert to list of events
        events = []
        for _, row in timeline.iterrows():
            events.append({
                'year': int(row['year']),
                'month': int(row['month']),
                'date': f"{int(row['year'])}-{int(row['month']):02d}-01",
                'type': row['type'],
                'count': int(row['count'])
            })

        output = {
            'events': events,
            'summary': {
                'total_mp': len(self.mp),
                'total_up': len(self.up),
                'date_coverage_mp': self.mp['date'].notna().sum(),
                'date_coverage_up': self.up['date'].notna().sum(),
            }
        }

        with open(output_path, 'w') as f:
            json.dump(output, f, indent=2)

        return output_path


def main():
    """Run temporal analysis on current data."""
    ROOT = os.path.dirname(os.path.dirname(__file__))
    DATA_DIR = os.path.join(ROOT, 'data', 'clean')
    OUT_DIR = os.path.join(ROOT, 'out')

    print("Loading data...")
    mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
    up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))

    print(f"Loaded {len(mp):,} MPs and {len(up):,} UPs")

    analyzer = TemporalAnalyzer(mp, up)

    print("\n" + "="*60)
    print("YEARLY TRENDS")
    print("="*60)
    yearly = analyzer.yearly_trends()
    # Show recent years
    recent = yearly[yearly['year'] >= 2000]
    print(recent.to_string(index=False))

    print("\n" + "="*60)
    print("MONTHLY SEASONALITY")
    print("="*60)
    monthly = analyzer.monthly_seasonality()
    print(monthly[['month_name', 'mp_count', 'up_count', 'mp_deviation_pct', 'up_deviation_pct']].to_string(index=False))

    print("\n" + "="*60)
    print("DAY OF WEEK PATTERNS")
    print("="*60)
    dow = analyzer.day_of_week_patterns()
    print(dow[['day_name', 'mp_count', 'up_count']].to_string(index=False))

    print("\n" + "="*60)
    print("ERA ANALYSIS")
    print("="*60)
    eras = analyzer.era_analysis()
    for era, stats in eras.items():
        print(f"  {era}: {stats['mp_count']:,} MPs, {stats['up_count']:,} UPs")

    print("\n" + "="*60)
    print("TEMPORAL CLUSTERS (MP)")
    print("="*60)
    clusters = analyzer.find_temporal_clusters(window_days=30, min_cases=10, case_type='mp')
    for c in clusters[:5]:
        print(f"  {c['start_date']} to {c['end_date']}: {c['case_count']} cases")
        print(f"    Top states: {c['top_states']}")

    print("\n" + "="*60)
    print("DATE RANGES")
    print("="*60)
    gaps = analyzer.gap_analysis()
    print(f"  MP range: {gaps['mp_date_range']['earliest']} to {gaps['mp_date_range']['latest']}")
    print(f"  UP range: {gaps['up_date_range']['earliest']} to {gaps['up_date_range']['latest']}")

    # Export timeline data
    timeline_path = os.path.join(OUT_DIR, 'timeline_data.json')
    analyzer.export_timeline_data(timeline_path)
    print(f"\nExported timeline data to: {timeline_path}")


if __name__ == '__main__':
    main()
