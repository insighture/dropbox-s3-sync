#!/usr/bin/env python3
"""Main entry point for dsync"""

import os
import sys
from dsync.config import AppConfig, print_current_config
from dsync.sync_engine import SyncEngine
from dotenv import load_dotenv

load_dotenv()

def main():
    try:
        # Load configuration
        config = AppConfig.load_config()

        # Print configuration for debugging (optional)
        if os.getenv('DEBUG_CONFIG', '').lower() == 'true':
            print_current_config(config)

        # Create temp directory
        os.makedirs(config.sync.temp_dir, exist_ok=True)

        # Check for dry run (realtime sync = true)
        dry_run = os.getenv('DRY_RUN', '').lower() == 'false'

        # Check sync direction
        sync_direction = os.getenv('SYNC_DIRECTION', 'dropbox_to_s3').lower()
        valid_directions = ['dropbox_to_s3', 's3_to_dropbox', 'bidirectional']

        if sync_direction not in valid_directions:
            print(f"Invalid SYNC_DIRECTION: {sync_direction}. Must be one of: {', '.join(valid_directions)}")
            sys.exit(1)

        # Check conflict strategy for bidirectional sync
        conflict_strategy = os.getenv('CONFLICT_STRATEGY', 'newer').lower()
        valid_strategies = ['newer', 'dropbox-wins', 's3-wins']

        if conflict_strategy not in valid_strategies:
            print(f"Invalid CONFLICT_STRATEGY: {conflict_strategy}. Must be one of: {', '.join(valid_strategies)}")
            sys.exit(1)

        # Initialize sync engine
        sync_engine = SyncEngine(config)

        # Run appropriate sync based on direction
        if sync_direction == 'bidirectional':
            result = sync_engine.run_bidirectional_sync(
                dry_run=dry_run,
                conflict_strategy=conflict_strategy
            )
        else:
            result = sync_engine.run_sync(
                dry_run=dry_run,
                direction=sync_direction
            )

        # Display results
        if result['success']:
            if result.get('dry_run'):
                print(f"DRY RUN completed in {result['duration']:.2f}s")
                if result.get('preview'):
                    print("Preview of changes:")
                    print(f"  Dropbox -> S3: {result['preview']['dropbox_to_s3']['stdout'][:100]}...")
                    print(f"  S3 -> Dropbox: {result['preview']['s3_to_dropbox']['stdout'][:100]}...")
            else:
                sync_type = result.get('sync_type', result.get('direction', 'sync'))
                print(f"{sync_type.replace('_', ' ').title()} completed successfully in {result['duration']:.2f}s")
        else:
            print(f"Sync failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)

    except Exception as e:
        print(f"Sync failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()