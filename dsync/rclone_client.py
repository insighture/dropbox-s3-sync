"""Rclone client wrapper for Python integration"""

import os
import json
import subprocess
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import AppConfig
from .path_mapper import PathMapper

logger = logging.getLogger(__name__)

class RcloneClient:
    """Wrapper for rclone operations with Python integration"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.rclone_config_path = self._setup_rclone_config()
    
    #creates rcone configuration files from environment variables
    def _setup_rclone_config(self) -> Path:
        """Create rclone config file from environment variables"""
        config_dir = Path.home() / '.config' / 'rclone'
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / 'rclone.conf'

        # Get access token (handles OAuth refresh automatically)
        try:
            access_token = self.config.dropbox.get_access_token()
        except Exception as e:
            logger.error(f"Failed to get Dropbox access token: {e}")
            if self.config.dropbox.use_oauth:
                raise ValueError(
                    "OAuth token not available. Please run the OAuth server first:\n"
                    "1. python oauth_app.py\n"
                    "2. Visit http://localhost:8000\n"
                    "3. Complete the authorization flow"
                )
            else:
                raise ValueError("Dropbox access token not configured")
            
        # Create rclone config content
        #s3 configuration
        access_key = self.config.s3.access_key or ""
        secret_key = self.config.s3.secret_key or ""
        region = self.config.s3.region or "us-east-1"

        config_content = f"""[dropbox]
type = dropbox
token = {{"access_token":"{access_token}","token_type":"bearer","expiry":"0001-01-01T00:00:00Z"}}

[s3]
type = s3
provider = AWS
env_auth = false
access_key_id = {access_key}
secret_access_key = {secret_key}
region = {region}
"""

        # Write config file
        config_path.write_text(config_content)
        logger.info(f"Created rclone config at {config_path}")

        if self.config.dropbox.use_oauth:
            logger.info("Using OAuth token for Dropbox authentication")
        else:
            logger.info("Using direct access token for Dropbox authentication")

        return config_path

    def _run_rclone(self, args: List[str], **kwargs) -> subprocess.CompletedProcess:
        """Run rclone command with proper configuration"""
        
        cmd = [
            'rclone',
            '--config', str(self.rclone_config_path),
            '--log-level', self.config.sync.log_level.lower(),
        ] + args

        logger.debug(f"Running rclone command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            **kwargs
        )

        # Log output
        if result.stdout:
            logger.info(f"Rclone stdout: {result.stdout.strip()}")
        if result.stderr:
            logger.warning(f"Rclone stderr: {result.stderr.strip()}")

        return result

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(subprocess.CalledProcessError),
        reraise=True
    )
    def sync(self, source: str = 'dropbox:', dest: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
        """Perform sync operation with comprehensive logging and error handling"""
        if dest is None:
            dest = f"s3:{self.config.s3.bucket_name}"

        args = ['sync', source, dest]

        # Add sync options
        if dry_run:
            args.append('--dry-run')

        # Performance options
        args.extend([
            '--transfers', '4',
            '--checkers', '8',
            '--retries', str(self.config.dropbox.max_retries),
            '--retries-sleep', f'{self.config.dropbox.retry_delay}s',
            '--progress',
            '--stats', '10s',
            '--log-format', 'json',
        ])

        # Batch processing
        if hasattr(self.config.sync, 'batch_size'):
            args.extend(['--max-transfer', f'{self.config.sync.batch_size * 10}M'])

        logger.info(f"Starting rclone sync: {source} -> {dest}")

        try:
            result = self._run_rclone(args)

            if result.returncode == 0:
                logger.info("Rclone sync completed successfully")
                return {
                    'success': True,
                    'return_code': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                logger.error(f"Rclone sync failed with return code {result.returncode}")
                raise subprocess.CalledProcessError(
                    result.returncode, result.args, result.stdout, result.stderr
                )

        except subprocess.CalledProcessError as e:
            logger.error(f"Rclone command failed: {e}")
            return {
                'success': False,
                'return_code': e.returncode,
                'stdout': e.stdout,
                'stderr': e.stderr,
                'error': str(e)
            }

    def list_files(self, remote: str, path: str = '', path_mapper: Optional[PathMapper] = None) -> List[Dict[str, Any]]:
        """List files in remote path using rclone with optional path mapping"""
        args = ['lsjson', f'{remote}:{path}']

        result = self._run_rclone(args)

        if result.returncode == 0:
            try:
                files = json.loads(result.stdout)

                # Apply path mapping if provided
                if path_mapper:
                    for file_info in files:
                        if 'Path' in file_info:
                            original_path = file_info['Path']
                            mapped_path = path_mapper.transform_path(original_path)
                            if mapped_path != original_path:
                                file_info['OriginalPath'] = original_path
                                file_info['Path'] = mapped_path
                                logger.debug(f"Mapped path: {original_path} -> {mapped_path}")

                return files
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse rclone output: {e}")
                return []
        else:
            logger.error(f"Failed to list files: {result.stderr}")
            return []

    def check_sync_needed(self, source: str = 'dropbox:', dest: Optional[str] = None) -> Dict[str, Any]:
        """Check what would be synced without actually doing it"""
        if dest is None:
            dest = f"s3:{self.config.s3.bucket_name}"

        args = ['check', source, dest, '--dry-run', '--combined', '-']

        result = self._run_rclone(args)

        return {
            'return_code': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr
        }

    def sync_s3_to_dropbox(self, dry_run: bool = False) -> Dict[str, Any]:
        """Sync from S3 to Dropbox (reverse direction)"""
        source = f"s3:{self.config.s3.bucket_name}"
        dest = 'dropbox:'

        args = ['sync', source, dest]

        # Add sync options
        if dry_run:
            args.append('--dry-run')

        # Performance options
        args.extend([
            '--transfers', '4',
            '--checkers', '8',
            '--retries', str(self.config.dropbox.max_retries),
            '--retries-sleep', f'{self.config.dropbox.retry_delay}s',
            '--progress',
            '--stats', '10s',
            '--log-format', 'json',
        ])

        logger.info(f"Starting S3 to Dropbox sync: {source} -> {dest}")

        try:
            result = self._run_rclone(args)

            if result.returncode == 0:
                logger.info("S3 to Dropbox sync completed successfully")
                return {
                    'success': True,
                    'return_code': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                logger.error(f"S3 to Dropbox sync failed with return code {result.returncode}")
                raise subprocess.CalledProcessError(
                    result.returncode, result.args, result.stdout, result.stderr
                )

        except subprocess.CalledProcessError as e:
            logger.error(f"S3 to Dropbox sync command failed: {e}")
            return {
                'success': False,
                'return_code': e.returncode,
                'stdout': e.stdout,
                'stderr': e.stderr,
                'error': str(e)
            }

    def check_bidirectional_sync(self) -> Dict[str, Any]:
        """Check what would be synced in both directions"""
        logger.info("Checking bidirectional sync status...")

        # Check Dropbox -> S3
        dropbox_to_s3 = self.check_sync_needed('dropbox:', f"s3:{self.config.s3.bucket_name}")

        # Check S3 -> Dropbox
        s3_to_dropbox = self.check_sync_needed(f"s3:{self.config.s3.bucket_name}", 'dropbox:')

        return {
            'dropbox_to_s3': dropbox_to_s3,
            's3_to_dropbox': s3_to_dropbox,
            'has_changes': dropbox_to_s3['return_code'] != 0 or s3_to_dropbox['return_code'] != 0
        }

    def bidirectional_sync(self, dry_run: bool = False, conflict_strategy: str = 'newer') -> Dict[str, Any]:
        """
        Perform bidirectional sync between Dropbox and S3

        Args:
            dry_run: If True, only show what would be synced
            conflict_strategy: How to handle conflicts ('newer', 'dropbox-wins', 's3-wins')
        """
        logger.info(f"Starting bidirectional sync (strategy: {conflict_strategy})")

        if dry_run:
            logger.info("DRY RUN - Previewing bidirectional changes...")
            preview = self.check_bidirectional_sync()
            return {
                'success': True,
                'dry_run': True,
                'preview': preview
            }

        results = []

        try:
            # Step 1: Sync Dropbox -> S3 (newer files)
            logger.info("Step 1: Syncing Dropbox -> S3")
            dropbox_result = self.sync(dry_run=False)
            results.append({'direction': 'dropbox_to_s3', 'result': dropbox_result})

            # Step 2: Sync S3 -> Dropbox (newer files)
            logger.info("Step 2: Syncing S3 -> Dropbox")
            s3_result = self.sync_s3_to_dropbox(dry_run=False)
            results.append({'direction': 's3_to_dropbox', 'result': s3_result})

            # Check if both succeeded
            success = all(r['result']['success'] for r in results)

            return {
                'success': success,
                'results': results,
                'strategy': conflict_strategy
            }

        except Exception as e:
            logger.error(f"Bidirectional sync failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'partial_results': results
            }

    def get_stats(self) -> Dict[str, Any]:
        """Get rclone statistics"""
        try:
            # This would require rclone's remote control feature
            # For now, return basic info
            return {
                'config_valid': self.rclone_config_path.exists(),
                'dropbox_remote': 'dropbox',
                's3_remote': 's3',
                'bucket': self.config.s3.bucket_name
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {'error': str(e)}

    def cleanup(self):
        """Clean up rclone configuration"""
        try:
            if self.rclone_config_path.exists():
                self.rclone_config_path.unlink()
                logger.info("Cleaned up rclone configuration")
        except Exception as e:
            logger.warning(f"Failed to cleanup rclone config: {e}")