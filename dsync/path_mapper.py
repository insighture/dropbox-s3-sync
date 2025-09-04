"""Path mapping utilities for transforming file paths between different naming schemes"""

import re
import logging
from typing import Optional, Dict, Any
from pathlib import Path

from .config import PathMapping

logger = logging.getLogger(__name__)

class PathMapper:
    """Handles path transformations between different naming schemes"""

    def __init__(self, mappings: list[PathMapping]):
        self.mappings = [m for m in mappings if m.enabled]
        self.compiled_mappings = []

        for mapping in self.mappings:
            try:
                compiled = {
                    'source_regex': re.compile(mapping.source_pattern),
                    'dest_pattern': mapping.dest_pattern,
                    'mapping': mapping
                }
                self.compiled_mappings.append(compiled)
                logger.debug(f"Compiled mapping: {mapping.source_pattern} -> {mapping.dest_pattern}")
            except re.error as e:
                logger.error(f"Invalid regex pattern in mapping: {mapping.source_pattern} - {e}")

    def transform_path(self, source_path: str) -> str:
        """
        Transform a source path to destination path using configured mappings

        Args:
            source_path: The source file path to transform

        Returns:
            The transformed destination path, or original path if no mapping applies
        """
        if not self.compiled_mappings:
            return source_path

        for mapping in self.compiled_mappings:
            match = mapping['source_regex'].search(source_path)
            if match:
                try:
                    # Use regex substitution with the destination pattern
                    dest_path = mapping['source_regex'].sub(mapping['dest_pattern'], source_path)

                    # Handle named groups in the destination pattern
                    if '{' in mapping['dest_pattern'] and '}' in mapping['dest_pattern']:
                        dest_path = mapping['dest_pattern'].format(**match.groupdict())

                    logger.debug(f"Transformed path: {source_path} -> {dest_path}")
                    return dest_path

                except (KeyError, ValueError) as e:
                    logger.error(f"Error applying mapping to {source_path}: {e}")
                    continue

        # No mapping applied, return original path
        return source_path

    def reverse_transform_path(self, dest_path: str) -> str:
        """
        Transform a destination path back to source path (reverse mapping)

        Args:
            dest_path: The destination path to reverse transform

        Returns:
            The reverse-transformed source path
        """
        # For reverse transformation, we need to work backwards
        # This is more complex and would require reverse regex patterns
        # For now, we'll implement a simple approach

        if not self.compiled_mappings:
            return dest_path

        # Try to reverse each mapping
        for mapping in self.compiled_mappings:
            try:
                # Simple reverse: if dest pattern contains source pattern elements, swap them
                source_pattern = mapping['mapping'].source_pattern
                dest_pattern = mapping['mapping'].dest_pattern

                # This is a simplified reverse - in production you'd want more sophisticated logic
                if 'user_uuid' in dest_pattern and 'user_uuid' in source_pattern:
                    # Swap UUID-based paths back to original names
                    # This would need to be customized based on your specific mapping rules
                    pass

            except Exception as e:
                logger.error(f"Error in reverse transformation: {e}")
                continue

        return dest_path

    def get_mapping_info(self) -> Dict[str, Any]:
        """Get information about configured mappings"""
        return {
            'enabled_mappings': len(self.mappings),
            'mappings': [
                {
                    'source_pattern': m.source_pattern,
                    'dest_pattern': m.dest_pattern,
                    'enabled': m.enabled
                }
                for m in self.mappings
            ]
        }

class ContentMatcher:
    """Handles file matching by content rather than filename"""

    def __init__(self):
        self.content_hashes = {}  # In production, this could be a database

    def get_file_hash(self, file_path: str) -> Optional[str]:
        """Get hash of file content"""
        try:
            import hashlib
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error hashing file {file_path}: {e}")
            return None

    def find_duplicate_by_content(self, target_hash: str) -> Optional[str]:
        """Find a file with matching content hash"""
        # In production, this would query a database or index
        return self.content_hashes.get(target_hash)

    def index_file(self, file_path: str, hash_value: str):
        """Index a file by its content hash"""
        self.content_hashes[hash_value] = file_path

def create_uuid_mapping() -> list[PathMapping]:
    """Create common UUID-based path mappings for docs/user_uuid/file_id.pdf patterns"""

    mappings = [
        # Transform user-friendly Dropbox names to UUID-based S3 structure
        # This mapping extracts filename and creates a UUID-based structure
        PathMapping(
            source_pattern=r"^(.*/)?(.+)\.pdf$",
            dest_pattern="docs/user123/{filename}.pdf",  # Simplified - replace user123 with actual UUID logic
            enabled=True
        ),

        # Transform UUID-based S3 structure to user-friendly Dropbox names
        PathMapping(
            source_pattern=r"^docs/([^/]+)/([^/]+)\.pdf$",
            dest_pattern="{filename}.pdf",  # Extract filename from UUID structure
            enabled=True
        ),

        # Handle different file extensions
        PathMapping(
            source_pattern=r"^(.*/)?(.+)\.(docx|xlsx|pptx)$",
            dest_pattern="docs/user123/{filename}.{extension}",
            enabled=True
        )
    ]

    return mappings

def create_advanced_uuid_mapping(user_uuid: str, file_id: str) -> list[PathMapping]:
    """Create advanced UUID-based mappings with actual UUID values"""

    mappings = [
        # Transform any PDF to UUID structure
        PathMapping(
            source_pattern=r"^(.*/)?(.+)\.pdf$",
            dest_pattern=f"docs/{user_uuid}/{file_id}.pdf",
            enabled=True
        ),

        # Transform UUID structure back to original names
        PathMapping(
            source_pattern=f"docs/{user_uuid}/{file_id}\\.pdf$",
            dest_pattern="{original_name}.pdf",
            enabled=True
        )
    ]

    return mappings

def create_custom_mapping(source_pattern: str, dest_pattern: str) -> PathMapping:
    """Create a custom path mapping"""
    return PathMapping(
        source_pattern=source_pattern,
        dest_pattern=dest_pattern,
        enabled=True
    )