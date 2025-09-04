"""File matching utilities for identifying same content with different names"""

import hashlib
import logging
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class FileMatcher:
    """Handles matching files with different names but same content"""

    def __init__(self):
        self.content_hashes: Dict[str, List[Dict[str, Any]]] = {}
        self.metadata_matches: Dict[str, Dict[str, Any]] = {}

    def calculate_file_hash(self, file_path: str, algorithm: str = 'md5') -> Optional[str]:
        """Calculate hash of file content"""
        try:
            hash_func = getattr(hashlib, algorithm)()
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {file_path}: {e}")
            return None

    def extract_metadata_from_path(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from file path using patterns"""
        metadata = {}

        # Pattern 1: UUID-based S3 structure
        import re
        uuid_pattern = r"docs/([a-f0-9-]{36})/([a-f0-9-]{36})\.(.+)$"
        match = re.search(uuid_pattern, file_path)
        if match:
            metadata.update({
                'user_uuid': match.group(1),
                'file_uuid': match.group(2),
                'extension': match.group(3),
                'source': 's3',
                'structure': 'uuid'
            })

        # Pattern 2: Department-based structure
        dept_pattern = r"docs/([^/]+)/(.+)\.(.+)$"
        match = re.search(dept_pattern, file_path)
        if match and match.group(1) in ['hr', 'finance', 'sales', 'marketing']:
            metadata.update({
                'department': match.group(1),
                'filename': match.group(2),
                'extension': match.group(3),
                'source': 's3',
                'structure': 'department'
            })

        # Pattern 3: Date-based structure
        date_pattern = r"(.*/)?(\d{4})/(\d{2})/(.+)\.(.+)$"
        match = re.search(date_pattern, file_path)
        if match:
            metadata.update({
                'year': match.group(2),
                'month': match.group(3),
                'filename': match.group(4),
                'extension': match.group(5),
                'source': 's3',
                'structure': 'date'
            })

        return metadata

    def find_matches_by_content(self, file_path: str, candidate_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find files with same content using hash comparison"""
        file_hash = self.calculate_file_hash(file_path)
        if not file_hash:
            return []

        matches = []
        for candidate in candidate_files:
            candidate_path = candidate.get('local_path') or candidate.get('path', '')
            if candidate_path and Path(candidate_path).exists():
                candidate_hash = self.calculate_file_hash(candidate_path)
                if candidate_hash and candidate_hash == file_hash:
                    matches.append({
                        **candidate,
                        'match_type': 'content_hash',
                        'match_confidence': 1.0,
                        'hash': file_hash
                    })

        return matches

    def find_matches_by_metadata(self, file_path: str, candidate_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find files with matching metadata patterns"""
        source_metadata = self.extract_metadata_from_path(file_path)
        matches = []

        for candidate in candidate_files:
            candidate_path = candidate.get('path', '')
            candidate_metadata = self.extract_metadata_from_path(candidate_path)

            # Compare metadata fields
            confidence = self._calculate_metadata_similarity(source_metadata, candidate_metadata)

            if confidence > 0.5:  # Threshold for considering it a match
                matches.append({
                    **candidate,
                    'match_type': 'metadata',
                    'match_confidence': confidence,
                    'source_metadata': source_metadata,
                    'candidate_metadata': candidate_metadata
                })

        return matches

    def find_matches_by_filename_pattern(self, file_path: str, candidate_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find files using filename pattern matching"""
        import re

        source_name = Path(file_path).stem.lower()
        matches = []

        # Common patterns that might indicate same file
        patterns = [
            # Remove common prefixes/suffixes
            (r'^(invoice|report|document|file)[-_]', ''),
            (r'[-_](draft|final|version|v\d+)$', ''),

            # Date patterns
            (r'\d{4}[-/]\d{2}[-/]\d{2}', 'DATE'),
            (r'\d{2}[-/]\d{2}[-/]\d{4}', 'DATE'),

            # Number patterns
            (r'#?\d+', 'NUMBER'),
            (r'(v|version|rev)\d+', 'VERSION'),
        ]

        # Normalize source filename
        normalized_source = source_name
        for pattern, replacement in patterns:
            normalized_source = re.sub(pattern, replacement, normalized_source, flags=re.IGNORECASE)

        # Remove extra spaces and punctuation
        normalized_source = re.sub(r'[^\w\s]', '', normalized_source)
        normalized_source = ' '.join(normalized_source.split())

        for candidate in candidate_files:
            candidate_path = candidate.get('path', '')
            candidate_name = Path(candidate_path).stem.lower()

            # Normalize candidate filename
            normalized_candidate = candidate_name
            for pattern, replacement in patterns:
                normalized_candidate = re.sub(pattern, replacement, normalized_candidate, flags=re.IGNORECASE)

            normalized_candidate = re.sub(r'[^\w\s]', '', normalized_candidate)
            normalized_candidate = ' '.join(normalized_candidate.split())

            # Calculate similarity
            similarity = self._calculate_string_similarity(normalized_source, normalized_candidate)

            if similarity > 0.7:  # High similarity threshold
                matches.append({
                    **candidate,
                    'match_type': 'filename_pattern',
                    'match_confidence': similarity,
                    'normalized_source': normalized_source,
                    'normalized_candidate': normalized_candidate
                })

        return matches

    def find_best_match(self, file_path: str, candidate_files: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find the best match for a file using multiple strategies"""
        all_matches = []

        # Try content-based matching first (most accurate)
        content_matches = self.find_matches_by_content(file_path, candidate_files)
        all_matches.extend(content_matches)

        # Then metadata-based matching
        metadata_matches = self.find_matches_by_metadata(file_path, candidate_files)
        all_matches.extend(metadata_matches)

        # Finally filename pattern matching
        pattern_matches = self.find_matches_by_filename_pattern(file_path, candidate_files)
        all_matches.extend(pattern_matches)

        if not all_matches:
            return None

        # Return the match with highest confidence
        best_match = max(all_matches, key=lambda x: x.get('match_confidence', 0))
        return best_match

    def _calculate_metadata_similarity(self, metadata1: Dict[str, Any], metadata2: Dict[str, Any]) -> float:
        """Calculate similarity between two metadata dictionaries"""
        if not metadata1 or not metadata2:
            return 0.0

        common_keys = set(metadata1.keys()) & set(metadata2.keys())
        if not common_keys:
            return 0.0

        matches = 0
        for key in common_keys:
            if metadata1[key] == metadata2[key]:
                matches += 1

        return matches / len(common_keys)

    def _calculate_string_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using simple algorithm"""
        if not str1 or not str2:
            return 0.0

        # Simple word-based similarity
        words1 = set(str1.lower().split())
        words2 = set(str2.lower().split())

        intersection = words1 & words2
        union = words1 | words2

        if not union:
            return 0.0

        return len(intersection) / len(union)

class MatchResult:
    """Represents a file matching result"""

    def __init__(self, source_file: str, matched_file: Optional[Dict[str, Any]], match_type: str, confidence: float):
        self.source_file = source_file
        self.matched_file = matched_file
        self.match_type = match_type
        self.confidence = confidence

    def to_dict(self) -> Dict[str, Any]:
        return {
            'source_file': self.source_file,
            'matched_file': self.matched_file,
            'match_type': self.match_type,
            'confidence': self.confidence
        }