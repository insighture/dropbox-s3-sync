import os
import logging
from typing import Optional, List
from pydantic import BaseModel, Field, ValidationError, field_validator
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

# manages Dropbox credentials, s3 settings and sync params
class DropboxConfig(BaseModel):
    """Dropbox configuration settings"""
    access_token: str = Field(..., description="Dropbox access token")
    max_retries: int = Field(3, gt=0, description="Maximum retry attempts for Dropbox operations")
    retry_delay: float = Field(1.0, gt=0, description="Initial retry delay in seconds")
    timeout: int = Field(300, gt=0, description="Request timeout in seconds")

    @field_validator('access_token')
    @classmethod
    def validate_access_token(cls, v: str) -> str:
        """Validate Dropbox access token format"""
        if not v.startswith(('sl.', 'pk.')):
            logger.warning("Dropbox access token format looks unusual")
        return v

class S3Config(BaseModel):
    """AWS S3 configuration settings"""
    access_key: str = Field(..., description="AWS access key ID")
    secret_key: str = Field(..., description="AWS secret access key")
    bucket_name: str = Field(..., description="S3 bucket name")
    region: str = Field(..., description="AWS region")
    max_retries: int = Field(3, gt=0, description="Maximum retry attempts for S3 operations")
    retry_delay: float = Field(1.0, gt=0, description="Initial retry delay in seconds")
    timeout: int = Field(300, gt=0, description="Request timeout in seconds")

class PathMapping(BaseModel):
    """Path mapping configuration for transforming file paths between services"""
    source_pattern: str = Field(..., description="Regex pattern to match source paths")
    dest_pattern: str = Field(..., description="Destination pattern with placeholders")
    enabled: bool = Field(True, description="Whether this mapping is enabled")

class SyncConfig(BaseModel):
    """Sync operation configuration settings"""
    batch_size: int = Field(10, gt=0, description="Number of files to process per batch")
    max_concurrent_downloads: int = Field(3, gt=0, description="Maximum concurrent downloads")
    temp_dir: str = Field('/tmp/dsync', description="Temporary directory for file operations")
    log_level: str = Field('INFO', description="Logging level")
    log_file: str = Field('/var/log/dsync/sync.log', description="Log file path")

    # Path mapping configuration
    enable_path_mapping: bool = Field(False, description="Enable path transformation between services")
    dropbox_to_s3_mappings: List[PathMapping] = Field(default_factory=list, description="Path mappings for Dropbox to S3")
    s3_to_dropbox_mappings: List[PathMapping] = Field(default_factory=list, description="Path mappings for S3 to Dropbox")

    # File matching configuration
    match_by_content: bool = Field(False, description="Match files by content hash instead of name")
    match_by_metadata: bool = Field(False, description="Match files by custom metadata fields")

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level"""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"LOG_LEVEL must be one of: {', '.join(valid_levels)}")
        return v.upper()

class AppConfig(BaseSettings):
    """Main application configuration"""
    dropbox: DropboxConfig = Field(...)
    s3: S3Config = Field(...)
    sync: SyncConfig = Field(...)

    class Config:
        """Pydantic configuration"""
        env_nested_delimiter = '__'
        case_sensitive = False

    @classmethod
    def load_config(cls) -> 'AppConfig':
        """Load configuration from environment variables"""
        try:
            # Check for required environment variables
            required_vars = [
                'DROPBOX_ACCESS_TOKEN',
                'AWS_ACCESS_KEY_ID',
                'AWS_SECRET_ACCESS_KEY',
                'S3_BUCKET_NAME',
                'AWS_REGION'
            ]

            missing_vars = [var for var in required_vars if not os.getenv(var)]
            if missing_vars:
                raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

            # Create nested configs from environment variables
            dropbox_config = DropboxConfig(
                access_token=os.getenv('DROPBOX_ACCESS_TOKEN', ''),
                max_retries=int(os.getenv('DROPBOX_MAX_RETRIES', '3')),
                retry_delay=float(os.getenv('DROPBOX_RETRY_DELAY', '1.0')),
                timeout=int(os.getenv('DROPBOX_TIMEOUT', '300'))
            )

            s3_config = S3Config(
                access_key=os.getenv('AWS_ACCESS_KEY_ID', ''),
                secret_key=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
                bucket_name=os.getenv('S3_BUCKET_NAME', ''),
                region=os.getenv('AWS_REGION', ''),
                max_retries=int(os.getenv('S3_MAX_RETRIES', '3')),
                retry_delay=float(os.getenv('S3_RETRY_DELAY', '1.0')),
                timeout=int(os.getenv('S3_TIMEOUT', '300'))
            )

            sync_config = SyncConfig(
                batch_size=int(os.getenv('SYNC_BATCH_SIZE', '10')),
                max_concurrent_downloads=int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '3')),
                temp_dir=os.getenv('SYNC_TEMP_DIR', '/tmp/dsync'),
                log_level=os.getenv('LOG_LEVEL', 'INFO'),
                log_file=os.getenv('LOG_FILE', '/var/log/dsync/sync.log'),
                enable_path_mapping=os.getenv('ENABLE_PATH_MAPPING', '').lower() == 'true',
                match_by_content=os.getenv('MATCH_BY_CONTENT', '').lower() == 'true',
                match_by_metadata=os.getenv('MATCH_BY_METADATA', '').lower() == 'true'
            )

            return cls(
                dropbox=dropbox_config,
                s3=s3_config,
                sync=sync_config
            )
        except ValidationError as e:
            logger.error(f"Configuration validation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Configuration loading error: {e}")
            raise

def print_config_template():
    """Print environment variable template for .env file"""
    template = """
# Dropbox Configuration
DROPBOX_ACCESS_TOKEN=your_dropbox_access_token_here
DROPBOX_MAX_RETRIES=3
DROPBOX_RETRY_DELAY=1.0
DROPBOX_TIMEOUT=300

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_here
AWS_SECRET_ACCESS_KEY=your_aws_secret_key_here
S3_BUCKET_NAME=your_s3_bucket_name
AWS_REGION=us-east-1
S3_MAX_RETRIES=3
S3_RETRY_DELAY=1.0
S3_TIMEOUT=300

# Sync Configuration
SYNC_BATCH_SIZE=10
MAX_CONCURRENT_DOWNLOADS=3
SYNC_TEMP_DIR=/tmp/dsync
LOG_LEVEL=INFO
LOG_FILE=/var/log/dsync/sync.log
"""
    print("Environment Variables Template:")
    print(template)

def print_current_config(config: AppConfig):
    """Print current configuration (masking sensitive data)"""
    print("\nCurrent Configuration:")
    print(f"Dropbox: token={'*' * 10}..., retries={config.dropbox.max_retries}")
    print(f"S3: bucket={config.s3.bucket_name}, region={config.s3.region}, retries={config.s3.max_retries}")
    print(f"Sync: batch_size={config.sync.batch_size}, log_level={config.sync.log_level}")
    print(f"Temp dir: {config.sync.temp_dir}")
    print(f"Log file: {config.sync.log_file}")