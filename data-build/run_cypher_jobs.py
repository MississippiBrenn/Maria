#!/usr/bin/env python3
"""
Neptune Query Functions

Query Neptune graph database for missing/unidentified person matches.
Uses Gremlin for graph traversal queries.
"""

import os
import json
from typing import List, Dict, Optional, Any
import argparse
from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError


class NeptuneQueryClient:
    """Client for querying Neptune graph database."""

    def __init__(self, neptune_endpoint: str, port: int = 8182):
        """
        Initialize Neptune query client.

        Args:
            neptune_endpoint: Neptune cluster endpoint
            port: Neptune port (default: 8182)
        """
        self.endpoint = f"wss://{neptune_endpoint}:{port}/gremlin"

        self.client = client.Client(
            self.endpoint,
            'g',
            message_serializer=serializer.GraphSONSerializersV2d0()
        )

    def execute_query(self, query: str) -> List[Any]:
        """
        Execute a Gremlin query.

        Args:
            query: Gremlin query string

        Returns:
            Query results
        """
        try:
            result_set = self.client.submit(query)
            return result_set.all().result()
        except GremlinServerError as e:
            print(f"Query error: {e}")
            raise

    def get_all_missing_persons(self, limit: int = 100) -> List[Dict]:
        """
        Get all missing person nodes.

        Args:
            limit: Maximum number of results

        Returns:
            List of missing person records
        """
        query = f"""
        g.V().hasLabel('MissingPerson')
         .limit({limit})
         .valueMap(true)
        """
        results = self.execute_query(query)
        return [self._format_vertex(v) for v in results]

    def get_all_unidentified_persons(self, limit: int = 100) -> List[Dict]:
        """
        Get all unidentified person nodes.

        Args:
            limit: Maximum number of results

        Returns:
            List of unidentified person records
        """
        query = f"""
        g.V().hasLabel('UnidentifiedPerson')
         .limit({limit})
         .valueMap(true)
        """
        results = self.execute_query(query)
        return [self._format_vertex(v) for v in results]

    def find_matches_for_mp(
        self,
        mp_id: str,
        min_score: float = 0.5,
        limit: int = 20
    ) -> List[Dict]:
        """
        Find potential matches for a missing person.

        Args:
            mp_id: Missing person ID (e.g., 'MP-001')
            min_score: Minimum match score threshold
            limit: Maximum number of matches

        Returns:
            List of matched unidentified persons with edges
        """
        query = f"""
        g.V('{mp_id}')
         .outE('NEAR')
         .has('score', gte({min_score}))
         .order().by('score', desc)
         .limit({limit})
         .project('edge', 'uid')
           .by(valueMap(true))
           .by(inV().valueMap(true))
        """
        results = self.execute_query(query)
        return [
            {
                'edge': self._format_edge(r['edge']),
                'uid': self._format_vertex(r['uid'])
            }
            for r in results
        ]

    def find_matches_for_uid(
        self,
        uid_id: str,
        min_score: float = 0.5,
        limit: int = 20
    ) -> List[Dict]:
        """
        Find potential matches for an unidentified person.

        Args:
            uid_id: Unidentified person ID (e.g., 'UID-101')
            min_score: Minimum match score threshold
            limit: Maximum number of matches

        Returns:
            List of matched missing persons with edges
        """
        query = f"""
        g.V('{uid_id}')
         .inE('NEAR')
         .has('score', gte({min_score}))
         .order().by('score', desc)
         .limit({limit})
         .project('edge', 'mp')
           .by(valueMap(true))
           .by(outV().valueMap(true))
        """
        results = self.execute_query(query)
        return [
            {
                'edge': self._format_edge(r['edge']),
                'mp': self._format_vertex(r['mp'])
            }
            for r in results
        ]

    def find_by_location(
        self,
        state: str,
        max_distance_km: float = 200,
        limit: int = 100
    ) -> List[Dict]:
        """
        Find all MP-UID matches within a state.

        Args:
            state: Two-letter state code (e.g., 'AR')
            max_distance_km: Maximum distance in kilometers
            limit: Maximum number of results

        Returns:
            List of edges with source and target nodes
        """
        query = f"""
        g.V().hasLabel('MissingPerson')
         .has('state', '{state}')
         .outE('NEAR')
         .has('km', lte({max_distance_km}))
         .limit({limit})
         .project('mp', 'edge', 'uid')
           .by(outV().valueMap(true))
           .by(valueMap(true))
           .by(inV().valueMap(true))
        """
        results = self.execute_query(query)
        return [
            {
                'mp': self._format_vertex(r['mp']),
                'edge': self._format_edge(r['edge']),
                'uid': self._format_vertex(r['uid'])
            }
            for r in results
        ]

    def find_by_sex(
        self,
        sex: str,
        limit: int = 100
    ) -> List[Dict]:
        """
        Find all matches for a specific sex.

        Args:
            sex: Sex ('M' or 'F')
            limit: Maximum number of results

        Returns:
            List of edges with source and target nodes
        """
        query = f"""
        g.V().hasLabel('MissingPerson')
         .has('sex', '{sex}')
         .outE('NEAR')
         .limit({limit})
         .project('mp', 'edge', 'uid')
           .by(outV().valueMap(true))
           .by(valueMap(true))
           .by(inV().valueMap(true))
        """
        results = self.execute_query(query)
        return [
            {
                'mp': self._format_vertex(r['mp']),
                'edge': self._format_edge(r['edge']),
                'uid': self._format_vertex(r['uid'])
            }
            for r in results
        ]

    def get_graph_stats(self) -> Dict:
        """
        Get statistics about the graph.

        Returns:
            Dictionary with node and edge counts
        """
        stats = {}

        # Count MissingPerson nodes
        mp_count_query = "g.V().hasLabel('MissingPerson').count()"
        stats['missing_persons'] = self.execute_query(mp_count_query)[0]

        # Count UnidentifiedPerson nodes
        uid_count_query = "g.V().hasLabel('UnidentifiedPerson').count()"
        stats['unidentified_persons'] = self.execute_query(uid_count_query)[0]

        # Count NEAR edges
        near_count_query = "g.E().hasLabel('NEAR').count()"
        stats['near_edges'] = self.execute_query(near_count_query)[0]

        # Count TIME_OVERLAP edges
        time_count_query = "g.E().hasLabel('TIME_OVERLAP').count()"
        stats['time_overlap_edges'] = self.execute_query(time_count_query)[0]

        # Total nodes
        total_nodes_query = "g.V().count()"
        stats['total_nodes'] = self.execute_query(total_nodes_query)[0]

        # Total edges
        total_edges_query = "g.E().count()"
        stats['total_edges'] = self.execute_query(total_edges_query)[0]

        return stats

    def _format_vertex(self, vertex: Dict) -> Dict:
        """
        Format vertex from Gremlin result to simple dict.

        Args:
            vertex: Gremlin vertex with cardinality format

        Returns:
            Simplified dict
        """
        formatted = {}
        for key, value in vertex.items():
            if key == 'id':
                formatted['id'] = value
            elif key == 'label':
                formatted['label'] = value
            elif isinstance(value, list) and len(value) > 0:
                # Extract first value from list (Gremlin returns [value])
                formatted[key] = value[0]
            else:
                formatted[key] = value
        return formatted

    def _format_edge(self, edge: Dict) -> Dict:
        """
        Format edge from Gremlin result to simple dict.

        Args:
            edge: Gremlin edge with cardinality format

        Returns:
            Simplified dict
        """
        return self._format_vertex(edge)

    def close(self):
        """Close the client connection."""
        self.client.close()


def main():
    """CLI interface for Neptune queries."""
    parser = argparse.ArgumentParser(
        description="Query Neptune graph database for missing persons matches"
    )
    parser.add_argument(
        "--neptune-endpoint",
        required=False,
        help="Neptune cluster endpoint (or set NEPTUNE_ENDPOINT env var)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8182,
        help="Neptune port (default: 8182)"
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Stats command
    subparsers.add_parser('stats', help='Get graph statistics')

    # Find MP matches
    mp_parser = subparsers.add_parser('find-mp', help='Find matches for a missing person')
    mp_parser.add_argument('mp_id', help='Missing person ID (e.g., MP-001)')
    mp_parser.add_argument('--min-score', type=float, default=0.5, help='Minimum score')
    mp_parser.add_argument('--limit', type=int, default=20, help='Max results')

    # Find UID matches
    uid_parser = subparsers.add_parser('find-uid', help='Find matches for an unidentified person')
    uid_parser.add_argument('uid_id', help='Unidentified person ID (e.g., UID-101)')
    uid_parser.add_argument('--min-score', type=float, default=0.5, help='Minimum score')
    uid_parser.add_argument('--limit', type=int, default=20, help='Max results')

    # Find by location
    loc_parser = subparsers.add_parser('find-state', help='Find matches by state')
    loc_parser.add_argument('state', help='Two-letter state code (e.g., AR)')
    loc_parser.add_argument('--max-distance', type=float, default=200, help='Max distance in km')
    loc_parser.add_argument('--limit', type=int, default=100, help='Max results')

    # Find by sex
    sex_parser = subparsers.add_parser('find-sex', help='Find matches by sex')
    sex_parser.add_argument('sex', choices=['M', 'F'], help='Sex')
    sex_parser.add_argument('--limit', type=int, default=100, help='Max results')

    args = parser.parse_args()

    neptune_endpoint = args.neptune_endpoint or os.getenv("NEPTUNE_ENDPOINT")
    if not neptune_endpoint:
        print("Error: Neptune endpoint required. Provide --neptune-endpoint or set NEPTUNE_ENDPOINT env var")
        return 1

    try:
        client = NeptuneQueryClient(neptune_endpoint, args.port)

        if args.command == 'stats':
            stats = client.get_graph_stats()
            print(json.dumps(stats, indent=2))

        elif args.command == 'find-mp':
            matches = client.find_matches_for_mp(
                args.mp_id,
                min_score=args.min_score,
                limit=args.limit
            )
            print(json.dumps(matches, indent=2))

        elif args.command == 'find-uid':
            matches = client.find_matches_for_uid(
                args.uid_id,
                min_score=args.min_score,
                limit=args.limit
            )
            print(json.dumps(matches, indent=2))

        elif args.command == 'find-state':
            matches = client.find_by_location(
                args.state,
                max_distance_km=args.max_distance,
                limit=args.limit
            )
            print(json.dumps(matches, indent=2))

        elif args.command == 'find-sex':
            matches = client.find_by_sex(
                args.sex,
                limit=args.limit
            )
            print(json.dumps(matches, indent=2))

        else:
            parser.print_help()

        client.close()
        return 0

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
