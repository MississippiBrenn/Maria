#!/usr/bin/env python3
"""
Geographic Analysis Module

Analyze geographic distribution of missing persons and unidentified remains.
Supports visualization, clustering, and hotspot identification.
"""

import os
import json
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

# US State coordinates for centroid-based mapping
STATE_CENTROIDS = {
    'AL': (32.806671, -86.791130), 'AK': (61.370716, -152.404419),
    'AZ': (33.729759, -111.431221), 'AR': (34.969704, -92.373123),
    'CA': (36.116203, -119.681564), 'CO': (39.059811, -105.311104),
    'CT': (41.597782, -72.755371), 'DE': (39.318523, -75.507141),
    'FL': (27.766279, -81.686783), 'GA': (33.040619, -83.643074),
    'HI': (21.094318, -157.498337), 'ID': (44.240459, -114.478828),
    'IL': (40.349457, -88.986137), 'IN': (39.849426, -86.258278),
    'IA': (42.011539, -93.210526), 'KS': (38.526600, -96.726486),
    'KY': (37.668140, -84.670067), 'LA': (31.169546, -91.867805),
    'ME': (44.693947, -69.381927), 'MD': (39.063946, -76.802101),
    'MA': (42.230171, -71.530106), 'MI': (43.326618, -84.536095),
    'MN': (45.694454, -93.900192), 'MS': (32.741646, -89.678696),
    'MO': (38.456085, -92.288368), 'MT': (46.921925, -110.454353),
    'NE': (41.125370, -98.268082), 'NV': (38.313515, -117.055374),
    'NH': (43.452492, -71.563896), 'NJ': (40.298904, -74.521011),
    'NM': (34.840515, -106.248482), 'NY': (42.165726, -74.948051),
    'NC': (35.630066, -79.806419), 'ND': (47.528912, -99.784012),
    'OH': (40.388783, -82.764915), 'OK': (35.565342, -96.928917),
    'OR': (44.572021, -122.070938), 'PA': (40.590752, -77.209755),
    'RI': (41.680893, -71.511780), 'SC': (33.856892, -80.945007),
    'SD': (44.299782, -99.438828), 'TN': (35.747845, -86.692345),
    'TX': (31.054487, -97.563461), 'UT': (40.150032, -111.862434),
    'VT': (44.045876, -72.710686), 'VA': (37.769337, -78.169968),
    'WA': (47.400902, -121.490494), 'WV': (38.491226, -80.954453),
    'WI': (44.268543, -89.616508), 'WY': (42.755966, -107.302490),
    'DC': (38.897438, -77.026817), 'PR': (18.220833, -66.590149),
}


class GeographicAnalyzer:
    """
    Analyze geographic distribution of missing persons and unidentified remains.
    """

    def __init__(self, mp_df: pd.DataFrame, up_df: pd.DataFrame):
        """
        Initialize with missing persons and unidentified remains DataFrames.

        Args:
            mp_df: DataFrame with columns [id, city, county, state, ...]
            up_df: DataFrame with columns [id, city, county, state, ...]
        """
        self.mp = mp_df.copy()
        self.up = up_df.copy()

        # Normalize state codes
        self.mp['state_code'] = self.mp['state'].str.strip().str.upper()
        self.up['state_code'] = self.up['state'].str.strip().str.upper()

    def state_summary(self) -> pd.DataFrame:
        """
        Generate state-by-state summary statistics.

        Returns:
            DataFrame with columns: state, mp_count, up_count, ratio, mp_per_100k
        """
        # US state populations (2023 estimates, in thousands)
        state_pop = {
            'CA': 39538, 'TX': 29145, 'FL': 21538, 'NY': 20201, 'PA': 13002,
            'IL': 12812, 'OH': 11799, 'GA': 10711, 'NC': 10439, 'MI': 10077,
            'NJ': 9288, 'VA': 8631, 'WA': 7614, 'AZ': 7151, 'MA': 7029,
            'TN': 6910, 'IN': 6785, 'MO': 6154, 'MD': 6177, 'WI': 5893,
            'CO': 5773, 'MN': 5706, 'SC': 5118, 'AL': 5024, 'LA': 4657,
            'KY': 4505, 'OR': 4237, 'OK': 3959, 'CT': 3605, 'UT': 3271,
            'IA': 3190, 'NV': 3104, 'AR': 3011, 'MS': 2961, 'KS': 2937,
            'NM': 2117, 'NE': 1961, 'ID': 1839, 'WV': 1793, 'HI': 1455,
            'NH': 1377, 'ME': 1362, 'MT': 1084, 'RI': 1097, 'DE': 989,
            'SD': 886, 'ND': 779, 'AK': 733, 'DC': 689, 'VT': 643, 'WY': 577,
            'PR': 3285,
        }

        mp_counts = self.mp['state_code'].value_counts()
        up_counts = self.up['state_code'].value_counts()

        all_states = set(mp_counts.index) | set(up_counts.index)
        rows = []

        for state in sorted(all_states):
            mp_count = mp_counts.get(state, 0)
            up_count = up_counts.get(state, 0)
            pop = state_pop.get(state, 0)

            rows.append({
                'state': state,
                'mp_count': mp_count,
                'up_count': up_count,
                'total_cases': mp_count + up_count,
                'mp_up_ratio': round(mp_count / up_count, 2) if up_count > 0 else None,
                'mp_per_100k': round(mp_count / pop * 100, 2) if pop > 0 else None,
                'up_per_100k': round(up_count / pop * 100, 2) if pop > 0 else None,
                'population_k': pop,
            })

        df = pd.DataFrame(rows)
        return df.sort_values('total_cases', ascending=False).reset_index(drop=True)

    def county_hotspots(self, top_n: int = 20, case_type: str = 'both') -> pd.DataFrame:
        """
        Identify counties with highest case concentrations.

        Args:
            top_n: Number of top counties to return
            case_type: 'mp', 'up', or 'both'

        Returns:
            DataFrame with county-level statistics
        """
        if case_type == 'mp':
            df = self.mp
        elif case_type == 'up':
            df = self.up
        else:
            df = pd.concat([
                self.mp[['state_code', 'county']].assign(type='MP'),
                self.up[['state_code', 'county']].assign(type='UP')
            ])

        # Create state_county key
        df = df.copy()
        df['county_clean'] = df['county'].str.strip().str.upper()
        df['state_county'] = df['state_code'] + ' - ' + df['county_clean']

        if case_type == 'both':
            counts = df.groupby('state_county').agg(
                total=('type', 'count'),
                mp_count=('type', lambda x: (x == 'MP').sum()),
                up_count=('type', lambda x: (x == 'UP').sum())
            ).reset_index()
        else:
            counts = df['state_county'].value_counts().reset_index()
            counts.columns = ['state_county', 'total']

        # Filter out empty counties
        counts = counts[counts['state_county'].str.len() > 5]

        return counts.head(top_n)

    def match_geography_analysis(self, matches_df: pd.DataFrame) -> Dict:
        """
        Analyze geographic patterns in scored matches.

        Args:
            matches_df: DataFrame with mp_id, up_id, score columns

        Returns:
            Dict with geographic match statistics
        """
        # Merge with case data
        matches = matches_df.merge(
            self.mp[['id', 'state_code', 'county', 'city']].rename(
                columns={'id': 'mp_id', 'state_code': 'mp_state',
                         'county': 'mp_county', 'city': 'mp_city'}
            ),
            on='mp_id'
        ).merge(
            self.up[['id', 'state_code', 'county', 'city']].rename(
                columns={'id': 'up_id', 'state_code': 'up_state',
                         'county': 'up_county', 'city': 'up_city'}
            ),
            on='up_id'
        )

        # Calculate match patterns
        same_state = (matches['mp_state'] == matches['up_state']).sum()
        same_county = (
            (matches['mp_state'] == matches['up_state']) &
            (matches['mp_county'].str.upper() == matches['up_county'].str.upper())
        ).sum()
        same_city = (
            (matches['mp_state'] == matches['up_state']) &
            (matches['mp_county'].str.upper() == matches['up_county'].str.upper()) &
            (matches['mp_city'].str.upper() == matches['up_city'].str.upper())
        ).sum()

        return {
            'total_matches': len(matches),
            'same_state': same_state,
            'same_state_pct': round(100 * same_state / len(matches), 1) if len(matches) > 0 else 0,
            'same_county': same_county,
            'same_county_pct': round(100 * same_county / len(matches), 1) if len(matches) > 0 else 0,
            'same_city': same_city,
            'same_city_pct': round(100 * same_city / len(matches), 1) if len(matches) > 0 else 0,
        }

    def export_geojson(self, output_path: str, case_type: str = 'both') -> str:
        """
        Export case locations as GeoJSON for mapping tools.

        Uses state centroids since we don't have precise coordinates.

        Args:
            output_path: Path to write GeoJSON file
            case_type: 'mp', 'up', or 'both'

        Returns:
            Path to created file
        """
        features = []

        if case_type in ('mp', 'both'):
            state_counts = self.mp['state_code'].value_counts()
            for state, count in state_counts.items():
                if state in STATE_CENTROIDS:
                    lat, lon = STATE_CENTROIDS[state]
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [lon, lat]
                        },
                        'properties': {
                            'state': state,
                            'case_type': 'MP',
                            'count': int(count),
                            'label': f'{state}: {count} missing persons'
                        }
                    })

        if case_type in ('up', 'both'):
            state_counts = self.up['state_code'].value_counts()
            for state, count in state_counts.items():
                if state in STATE_CENTROIDS:
                    lat, lon = STATE_CENTROIDS[state]
                    # Offset slightly if both types to avoid overlap
                    if case_type == 'both':
                        lon += 0.5
                    features.append({
                        'type': 'Feature',
                        'geometry': {
                            'type': 'Point',
                            'coordinates': [lon, lat]
                        },
                        'properties': {
                            'state': state,
                            'case_type': 'UP',
                            'count': int(count),
                            'label': f'{state}: {count} unidentified remains'
                        }
                    })

        geojson = {
            'type': 'FeatureCollection',
            'features': features
        }

        with open(output_path, 'w') as f:
            json.dump(geojson, f, indent=2)

        return output_path

    def generate_state_heatmap_data(self) -> Dict[str, Dict]:
        """
        Generate data for state-level heatmap visualization.

        Returns:
            Dict mapping state codes to their statistics
        """
        summary = self.state_summary()

        result = {}
        for _, row in summary.iterrows():
            result[row['state']] = {
                'mp_count': int(row['mp_count']),
                'up_count': int(row['up_count']),
                'total': int(row['total_cases']),
                'mp_per_100k': row['mp_per_100k'],
                'up_per_100k': row['up_per_100k'],
                'coords': STATE_CENTROIDS.get(row['state'], (0, 0))
            }

        return result

    def find_geographic_clusters(self, min_cluster_size: int = 10) -> List[Dict]:
        """
        Identify geographic clusters of cases.

        Simple approach: counties with high case concentrations.

        Args:
            min_cluster_size: Minimum cases to be considered a cluster

        Returns:
            List of cluster definitions
        """
        clusters = []

        for case_type, df in [('MP', self.mp), ('UP', self.up)]:
            df = df.copy()
            df['county_clean'] = df['county'].str.strip().str.upper()

            # Group by state + county
            grouped = df.groupby(['state_code', 'county_clean']).size().reset_index(name='count')
            grouped = grouped[grouped['count'] >= min_cluster_size]

            for _, row in grouped.iterrows():
                clusters.append({
                    'type': case_type,
                    'state': row['state_code'],
                    'county': row['county_clean'],
                    'count': int(row['count']),
                    'density': 'high' if row['count'] >= min_cluster_size * 2 else 'medium'
                })

        return sorted(clusters, key=lambda x: x['count'], reverse=True)


def main():
    """Run geographic analysis on current data."""
    import os

    ROOT = os.path.dirname(os.path.dirname(__file__))
    DATA_DIR = os.path.join(ROOT, 'data', 'clean')
    OUT_DIR = os.path.join(ROOT, 'out')

    print("Loading data...")
    mp = pd.read_csv(os.path.join(DATA_DIR, 'MP_master.csv'))
    up = pd.read_csv(os.path.join(DATA_DIR, 'UP_master.csv'))

    print(f"Loaded {len(mp):,} MPs and {len(up):,} UPs")

    analyzer = GeographicAnalyzer(mp, up)

    print("\n" + "="*60)
    print("STATE SUMMARY")
    print("="*60)
    summary = analyzer.state_summary()
    print(summary.head(15).to_string(index=False))

    print("\n" + "="*60)
    print("TOP COUNTY HOTSPOTS")
    print("="*60)
    hotspots = analyzer.county_hotspots(top_n=15)
    print(hotspots.to_string(index=False))

    print("\n" + "="*60)
    print("GEOGRAPHIC CLUSTERS")
    print("="*60)
    clusters = analyzer.find_geographic_clusters(min_cluster_size=50)
    for c in clusters[:10]:
        print(f"  {c['type']} cluster: {c['state']} - {c['county']} ({c['count']} cases)")

    # Export GeoJSON
    geojson_path = os.path.join(OUT_DIR, 'cases_map.geojson')
    analyzer.export_geojson(geojson_path)
    print(f"\nExported GeoJSON to: {geojson_path}")

    # Export state heatmap data
    heatmap_path = os.path.join(OUT_DIR, 'state_heatmap.json')
    heatmap_data = analyzer.generate_state_heatmap_data()
    with open(heatmap_path, 'w') as f:
        json.dump(heatmap_data, f, indent=2)
    print(f"Exported heatmap data to: {heatmap_path}")


if __name__ == '__main__':
    main()
