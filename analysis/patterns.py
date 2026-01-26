#!/usr/bin/env python3
"""
Pattern Identification Module

Identify patterns across geographic, demographic, and temporal dimensions.
Includes clustering algorithms and anomaly detection.
"""

import os
import json
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Set
from itertools import combinations
import pandas as pd
import numpy as np


class PatternAnalyzer:
    """
    Identify patterns in missing persons and unidentified remains data.
    """

    def __init__(self, mp_df: pd.DataFrame, up_df: pd.DataFrame):
        """
        Initialize with missing persons and unidentified remains DataFrames.

        Args:
            mp_df: Missing persons DataFrame
            up_df: Unidentified remains DataFrame
        """
        self.mp = mp_df.copy()
        self.up = up_df.copy()

        # Parse dates
        self.mp['date'] = pd.to_datetime(self.mp['last_seen_date'], errors='coerce')
        self.up['date'] = pd.to_datetime(self.up['found_date'], errors='coerce')

        # Normalize categorical columns
        for col in ['sex', 'race', 'state']:
            self.mp[f'{col}_norm'] = self.mp[col].str.strip().str.upper()
            self.up[f'{col}_norm'] = self.up[col].str.strip().str.upper()

    def demographic_profiles(self) -> Dict[str, pd.DataFrame]:
        """
        Analyze demographic distributions for MP and UP.

        Returns:
            Dict with 'mp' and 'up' DataFrames showing demographic breakdowns
        """
        profiles = {}

        for name, df in [('mp', self.mp), ('up', self.up)]:
            # Sex distribution
            sex_dist = df['sex_norm'].value_counts(normalize=True).round(3)

            # Race distribution
            race_dist = df['race_norm'].value_counts(normalize=True).round(3)

            # Age distribution (buckets)
            df = df.copy()
            df['age_bucket'] = pd.cut(
                df['age_min'],
                bins=[0, 5, 12, 18, 30, 50, 70, 100],
                labels=['0-5', '6-12', '13-18', '19-30', '31-50', '51-70', '70+']
            )
            age_dist = df['age_bucket'].value_counts(normalize=True).round(3)

            profiles[name] = {
                'sex': sex_dist.to_dict(),
                'race': race_dist.to_dict(),
                'age': age_dist.to_dict(),
                'total_count': len(df)
            }

        return profiles

    def demographic_comparison(self) -> pd.DataFrame:
        """
        Compare demographic distributions between MP and UP.

        Returns:
            DataFrame showing distribution differences
        """
        profiles = self.demographic_profiles()

        comparisons = []

        # Sex comparison
        for sex in set(profiles['mp']['sex'].keys()) | set(profiles['up']['sex'].keys()):
            mp_pct = profiles['mp']['sex'].get(sex, 0) * 100
            up_pct = profiles['up']['sex'].get(sex, 0) * 100
            comparisons.append({
                'category': 'Sex',
                'value': sex,
                'mp_pct': round(mp_pct, 1),
                'up_pct': round(up_pct, 1),
                'difference': round(mp_pct - up_pct, 1)
            })

        # Race comparison
        for race in set(profiles['mp']['race'].keys()) | set(profiles['up']['race'].keys()):
            mp_pct = profiles['mp']['race'].get(race, 0) * 100
            up_pct = profiles['up']['race'].get(race, 0) * 100
            if mp_pct > 1 or up_pct > 1:  # Only show significant categories
                comparisons.append({
                    'category': 'Race',
                    'value': race,
                    'mp_pct': round(mp_pct, 1),
                    'up_pct': round(up_pct, 1),
                    'difference': round(mp_pct - up_pct, 1)
                })

        # Age comparison
        for age in set(profiles['mp']['age'].keys()) | set(profiles['up']['age'].keys()):
            mp_pct = profiles['mp']['age'].get(age, 0) * 100
            up_pct = profiles['up']['age'].get(age, 0) * 100
            comparisons.append({
                'category': 'Age',
                'value': str(age),
                'mp_pct': round(mp_pct, 1),
                'up_pct': round(up_pct, 1),
                'difference': round(mp_pct - up_pct, 1)
            })

        return pd.DataFrame(comparisons)

    def find_demographic_clusters(self, min_cluster_size: int = 50) -> List[Dict]:
        """
        Find clusters of cases with similar demographics.

        Uses simple grouping approach (no ML dependencies).

        Args:
            min_cluster_size: Minimum cases to form a cluster

        Returns:
            List of demographic cluster definitions
        """
        clusters = []

        for case_type, df in [('MP', self.mp), ('UP', self.up)]:
            df = df.copy()

            # Create age bucket
            df['age_bucket'] = pd.cut(
                df['age_min'],
                bins=[0, 18, 35, 55, 100],
                labels=['Minor', 'Young Adult', 'Adult', 'Senior']
            )

            # Group by demographics
            grouped = df.groupby(['sex_norm', 'race_norm', 'age_bucket', 'state_norm']).size()
            grouped = grouped[grouped >= min_cluster_size].reset_index(name='count')

            for _, row in grouped.iterrows():
                clusters.append({
                    'type': case_type,
                    'sex': row['sex_norm'],
                    'race': row['race_norm'],
                    'age_group': str(row['age_bucket']),
                    'state': row['state_norm'],
                    'count': int(row['count']),
                    'profile': f"{row['sex_norm']} {row['race_norm']} {row['age_bucket']} in {row['state_norm']}"
                })

        # Sort by count
        clusters.sort(key=lambda x: x['count'], reverse=True)

        return clusters

    def find_spatiotemporal_patterns(
        self,
        time_window_days: int = 90,
        min_cases: int = 5
    ) -> List[Dict]:
        """
        Find patterns where multiple cases occur in same location + time window.

        Args:
            time_window_days: Days to consider "same time"
            min_cases: Minimum cases for a pattern

        Returns:
            List of spatiotemporal patterns
        """
        patterns = []

        for case_type, df in [('MP', self.mp), ('UP', self.up)]:
            df = df[df['date'].notna()].copy()
            df['county_norm'] = df['county'].str.strip().str.upper()

            # Group by state + county
            for (state, county), group in df.groupby(['state_norm', 'county_norm']):
                if len(group) < min_cases:
                    continue

                group = group.sort_values('date')

                # Sliding window for temporal clusters
                i = 0
                while i < len(group):
                    window_start = group.iloc[i]['date']
                    window_end = window_start + pd.Timedelta(days=time_window_days)

                    in_window = group[(group['date'] >= window_start) & (group['date'] < window_end)]

                    if len(in_window) >= min_cases:
                        # Demographics breakdown
                        sex_dist = in_window['sex_norm'].value_counts().to_dict()
                        race_dist = in_window['race_norm'].value_counts().head(2).to_dict()

                        patterns.append({
                            'type': case_type,
                            'state': state,
                            'county': county,
                            'start_date': window_start.strftime('%Y-%m-%d'),
                            'end_date': window_end.strftime('%Y-%m-%d'),
                            'case_count': len(in_window),
                            'sex_distribution': sex_dist,
                            'race_distribution': race_dist,
                            'case_ids': in_window['id'].tolist()
                        })

                        # Skip past this cluster
                        i += len(in_window)
                    else:
                        i += 1

        # Sort by case count
        patterns.sort(key=lambda x: x['case_count'], reverse=True)

        return patterns[:100]  # Top 100 patterns

    def match_pattern_analysis(self, matches_df: pd.DataFrame) -> Dict:
        """
        Analyze patterns in high-scoring matches.

        Args:
            matches_df: DataFrame with mp_id, up_id, final_score, etc.

        Returns:
            Dict with match pattern statistics
        """
        # Merge with case data
        matches = matches_df.merge(
            self.mp[['id', 'sex_norm', 'race_norm', 'state_norm', 'age_min']].rename(
                columns={'id': 'mp_id', 'sex_norm': 'mp_sex', 'race_norm': 'mp_race',
                         'state_norm': 'mp_state', 'age_min': 'mp_age'}
            ),
            on='mp_id'
        ).merge(
            self.up[['id', 'sex_norm', 'race_norm', 'state_norm', 'age_min']].rename(
                columns={'id': 'up_id', 'sex_norm': 'up_sex', 'race_norm': 'up_race',
                         'state_norm': 'up_state', 'age_min': 'up_age'}
            ),
            on='up_id'
        )

        # Demographic patterns
        same_race = (matches['mp_race'] == matches['up_race']).sum()
        same_state = (matches['mp_state'] == matches['up_state']).sum()

        # Age difference distribution
        matches['age_diff'] = abs(matches['mp_age'] - matches['up_age'])
        age_diff_mean = matches['age_diff'].mean()
        age_diff_median = matches['age_diff'].median()

        # Most common demographic profiles in matches
        matches['mp_profile'] = matches['mp_sex'] + '-' + matches['mp_race']
        profile_counts = matches['mp_profile'].value_counts().head(10).to_dict()

        return {
            'total_matches': len(matches),
            'race_agreement': {
                'same_race': int(same_race),
                'same_race_pct': round(100 * same_race / len(matches), 1) if len(matches) > 0 else 0
            },
            'state_agreement': {
                'same_state': int(same_state),
                'same_state_pct': round(100 * same_state / len(matches), 1) if len(matches) > 0 else 0
            },
            'age_difference': {
                'mean': round(age_diff_mean, 1) if not pd.isna(age_diff_mean) else None,
                'median': round(age_diff_median, 1) if not pd.isna(age_diff_median) else None
            },
            'top_demographic_profiles': profile_counts
        }

    def find_anomalies(self, threshold_std: float = 2.0) -> Dict[str, List]:
        """
        Find statistical anomalies in the data.

        Args:
            threshold_std: Number of standard deviations for anomaly

        Returns:
            Dict with anomaly lists by category
        """
        anomalies = {'geographic': [], 'temporal': [], 'demographic': []}

        # Geographic anomalies: states with unusual MP/UP ratios
        mp_by_state = self.mp['state_norm'].value_counts()
        up_by_state = self.up['state_norm'].value_counts()

        ratios = {}
        for state in set(mp_by_state.index) & set(up_by_state.index):
            if up_by_state[state] >= 10:  # Minimum sample
                ratios[state] = mp_by_state[state] / up_by_state[state]

        if ratios:
            ratio_mean = np.mean(list(ratios.values()))
            ratio_std = np.std(list(ratios.values()))

            for state, ratio in ratios.items():
                z_score = (ratio - ratio_mean) / ratio_std if ratio_std > 0 else 0
                if abs(z_score) > threshold_std:
                    anomalies['geographic'].append({
                        'state': state,
                        'mp_count': int(mp_by_state[state]),
                        'up_count': int(up_by_state[state]),
                        'ratio': round(ratio, 2),
                        'z_score': round(z_score, 2),
                        'type': 'high_mp_ratio' if z_score > 0 else 'low_mp_ratio'
                    })

        # Temporal anomalies: months with unusual case counts
        for case_type, df in [('MP', self.mp), ('UP', self.up)]:
            df = df[df['date'].notna()].copy()
            monthly = df.groupby(df['date'].dt.month).size()

            if len(monthly) > 0:
                month_mean = monthly.mean()
                month_std = monthly.std()

                for month, count in monthly.items():
                    z_score = (count - month_mean) / month_std if month_std > 0 else 0
                    if abs(z_score) > threshold_std:
                        anomalies['temporal'].append({
                            'case_type': case_type,
                            'month': int(month),
                            'count': int(count),
                            'z_score': round(z_score, 2),
                            'type': 'high_count' if z_score > 0 else 'low_count'
                        })

        return anomalies

    def correlation_matrix(self) -> pd.DataFrame:
        """
        Generate correlation analysis between categorical variables.

        Uses chi-square test approximation for categorical correlation.

        Returns:
            DataFrame with correlation-like statistics
        """
        results = []

        # For each pair of categorical variables, calculate Cramer's V
        categorical_cols = ['sex_norm', 'race_norm', 'state_norm']

        for df_name, df in [('MP', self.mp), ('UP', self.up)]:
            for col1, col2 in combinations(categorical_cols, 2):
                # Create contingency table
                contingency = pd.crosstab(df[col1], df[col2])

                # Calculate Cramer's V (simplified)
                n = contingency.sum().sum()
                chi2 = 0
                for i in range(contingency.shape[0]):
                    for j in range(contingency.shape[1]):
                        expected = (contingency.iloc[i].sum() * contingency.iloc[:, j].sum()) / n
                        if expected > 0:
                            chi2 += (contingency.iloc[i, j] - expected) ** 2 / expected

                min_dim = min(contingency.shape[0] - 1, contingency.shape[1] - 1)
                cramers_v = np.sqrt(chi2 / (n * min_dim)) if min_dim > 0 and n > 0 else 0

                results.append({
                    'dataset': df_name,
                    'variable_1': col1.replace('_norm', ''),
                    'variable_2': col2.replace('_norm', ''),
                    'cramers_v': round(cramers_v, 3),
                    'interpretation': 'strong' if cramers_v > 0.3 else ('moderate' if cramers_v > 0.1 else 'weak')
                })

        return pd.DataFrame(results)

    def export_pattern_report(self, output_path: str) -> str:
        """
        Export comprehensive pattern analysis report.

        Args:
            output_path: Path to write JSON report

        Returns:
            Path to created file
        """
        report = {
            'demographic_profiles': self.demographic_profiles(),
            'demographic_comparison': self.demographic_comparison().to_dict(orient='records'),
            'demographic_clusters': self.find_demographic_clusters(min_cluster_size=30)[:20],
            'spatiotemporal_patterns': self.find_spatiotemporal_patterns()[:20],
            'anomalies': self.find_anomalies(),
            'correlations': self.correlation_matrix().to_dict(orient='records'),
        }

        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        return output_path


def main():
    """Run pattern analysis on current data."""
    ROOT = os.path.dirname(os.path.dirname(__file__))
    DATA_DIR = os.path.join(ROOT, 'data', 'clean')
    OUT_DIR = os.path.join(ROOT, 'out')

    print("Loading data...")
    mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
    up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))

    print(f"Loaded {len(mp):,} MPs and {len(up):,} UPs")

    analyzer = PatternAnalyzer(mp, up)

    print("\n" + "="*60)
    print("DEMOGRAPHIC COMPARISON")
    print("="*60)
    comparison = analyzer.demographic_comparison()
    print(comparison.to_string(index=False))

    print("\n" + "="*60)
    print("TOP DEMOGRAPHIC CLUSTERS")
    print("="*60)
    clusters = analyzer.find_demographic_clusters(min_cluster_size=30)
    for c in clusters[:10]:
        print(f"  {c['type']}: {c['profile']} ({c['count']} cases)")

    print("\n" + "="*60)
    print("SPATIOTEMPORAL PATTERNS")
    print("="*60)
    patterns = analyzer.find_spatiotemporal_patterns(time_window_days=90, min_cases=5)
    for p in patterns[:5]:
        print(f"  {p['type']} in {p['state']}/{p['county']}: {p['case_count']} cases")
        print(f"    {p['start_date']} to {p['end_date']}")
        print(f"    Sex: {p['sex_distribution']}")

    print("\n" + "="*60)
    print("ANOMALIES")
    print("="*60)
    anomalies = analyzer.find_anomalies()
    print("Geographic anomalies:")
    for a in anomalies['geographic'][:5]:
        print(f"  {a['state']}: ratio={a['ratio']}, z={a['z_score']} ({a['type']})")

    print("\n" + "="*60)
    print("VARIABLE CORRELATIONS")
    print("="*60)
    corr = analyzer.correlation_matrix()
    print(corr.to_string(index=False))

    # Export report
    report_path = os.path.join(OUT_DIR, 'pattern_report.json')
    analyzer.export_pattern_report(report_path)
    print(f"\nExported pattern report to: {report_path}")


if __name__ == '__main__':
    main()
