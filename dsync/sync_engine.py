"""Main sync engine that orchestrates the sync process using rclone"""

import logging
import time
from typing import Dict, Any, List
from dsync.config import AppConfig
from dsync.rclone_client import RcloneClient
from dsync.path_mapper import PathMapper, ContentMatcher

logger = logging.getLogger(__name__)

class SyncEngine:
    """Main sync engine that coordinates operations using rclone"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.rclone_client = RcloneClient(config)
        self.start_time = None

        # Initialize path mapping if enabled
        if config.sync.enable_path_mapping:
            if config.sync.dropbox_to_s3_mappings:
                self.dropbox_to_s3_mapper = PathMapper(config.sync.dropbox_to_s3_mappings)
            else:
                self.dropbox_to_s3_mapper = None

            if config.sync.s3_to_dropbox_mappings:
                self.s3_to_dropbox_mapper = PathMapper(config.sync.s3_to_dropbox_mappings)
            else:
                self.s3_to_dropbox_mapper = None
        else:
            self.dropbox_to_s3_mapper = None
            self.s3_to_dropbox_mapper = None

        # Initialize content matcher if enabled
        self.content_matcher = ContentMatcher() if config.sync.match_by_content else None

    def run_sync(self, dry_run: bool = False, direction: str = 'dropbox_to_s3') -> Dict[str, Any]:
        """Run the complete sync process using rclone"""
        self.start_time = time.time()

        if direction == 'dropbox_to_s3':
            logger.info("Starting Dropbox to S3 sync with rclone")
            sync_method = self.rclone_client.sync
            source = 'dropbox:'
            dest = f"s3:{self.config.s3.bucket_name}"
        elif direction == 's3_to_dropbox':
            logger.info("Starting S3 to Dropbox sync with rclone")
            sync_method = self.rclone_client.sync_s3_to_dropbox
            source = f"s3:{self.config.s3.bucket_name}"
            dest = 'dropbox:'
        else:
            raise ValueError(f"Invalid direction: {direction}. Must be 'dropbox_to_s3' or 's3_to_dropbox'")

        try:
            # Optional: Check what would be synced
            if not dry_run:
                logger.info("Checking sync status...")
                # Note: Path mapping for check_sync_needed would require more complex implementation
                # For now, we'll check without mapping and log the limitation
                if self.config.sync.enable_path_mapping:
                    logger.warning("Path mapping enabled - sync preview may not reflect mapped paths")
                check_result = self.rclone_client.check_sync_needed(source, dest)
                if check_result['stdout']:
                    logger.info(f"Sync preview: {check_result['stdout'][:200]}...")

            # Apply path mapping if enabled
            mapped_source = source
            mapped_dest = dest

            if self.config.sync.enable_path_mapping:
                if direction == 'dropbox_to_s3' and self.dropbox_to_s3_mapper:
                    # For Dropbox to S3, we might want to transform paths
                    logger.info("Path mapping enabled for Dropbox -> S3 direction")
                    # Note: rclone handles the actual path transformation during sync
                elif direction == 's3_to_dropbox' and self.s3_to_dropbox_mapper:
                    logger.info("Path mapping enabled for S3 -> Dropbox direction")

            # Perform the actual sync
            sync_result = sync_method(dry_run=dry_run)

            # Calculate duration
            duration = time.time() - self.start_time

            result = {
                'success': sync_result['success'],
                'duration': duration,
                'dry_run': dry_run,
                'direction': direction,
                'source': source,
                'destination': dest,
                'path_mapping_enabled': self.config.sync.enable_path_mapping,
                'return_code': sync_result.get('return_code'),
                'stdout': sync_result.get('stdout', ''),
                'stderr': sync_result.get('stderr', ''),
            }

            if sync_result['success']:
                logger.info(f"Sync completed successfully in {duration:.2f}s")
            else:
                logger.error(f"Sync failed after {duration:.2f}s")
                if sync_result.get('error'):
                    result['error'] = sync_result['error']

            return result

        except Exception as e:
            duration = time.time() - self.start_time
            logger.error(f"Critical error during sync after {duration:.2f}s: {e}")
            return {
                'success': False,
                'duration': duration,
                'dry_run': dry_run,
                'direction': direction,
                'error': str(e)
            }
        finally:
            # Cleanup rclone config
            self.rclone_client.cleanup()

    def run_bidirectional_sync(self, dry_run: bool = False, conflict_strategy: str = 'newer') -> Dict[str, Any]:
        """Run bidirectional sync between Dropbox and S3"""
        self.start_time = time.time()
        logger.info("Starting bidirectional sync with rclone")

        try:
            # Perform bidirectional sync
            sync_result = self.rclone_client.bidirectional_sync(
                dry_run=dry_run,
                conflict_strategy=conflict_strategy
            )

            # Calculate duration
            duration = time.time() - self.start_time

            result = {
                'success': sync_result['success'],
                'duration': duration,
                'dry_run': dry_run,
                'sync_type': 'bidirectional',
                'conflict_strategy': conflict_strategy,
                **sync_result
            }

            if sync_result['success']:
                logger.info(f"Bidirectional sync completed successfully in {duration:.2f}s")
            else:
                logger.error(f"Bidirectional sync failed after {duration:.2f}s")

            return result

        except Exception as e:
            duration = time.time() - self.start_time
            logger.error(f"Critical error during bidirectional sync after {duration:.2f}s: {e}")
            return {
                'success': False,
                'duration': duration,
                'dry_run': dry_run,
                'sync_type': 'bidirectional',
                'error': str(e)
            }
        finally:
            # Cleanup rclone config
            self.rclone_client.cleanup()

    def get_stats(self) -> Dict[str, Any]:
        """Get sync statistics"""
        return self.rclone_client.get_stats()

    def list_source_files(self, limit: int = 10) -> List[Dict[str, Any]]:
        """List files from source for inspection"""
        files = self.rclone_client.list_files('dropbox')
        return files[:limit] if limit > 0 else files