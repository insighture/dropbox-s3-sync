import os
import logging
from typing import Optional, List
from pydantic import BaseModel, Field, ValidationError, field_validator
from pydantic_settings import BaseSettings
import requests
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class DropboxOAuthConfig(BaseModel):
    """Dropbox OAuth configuration settings"""
    client_id: str = Field(..., description="Dropbox client ID")
    client_secret: str = Field(..., description="Dropbox client secret")
    redirect_uri: str = Field(..., description="OAuth redirect URI")
    token_file: str = Field("TOKEN.txt", description="File to store tokens")


# manages Dropbox credentials, s3 settings and sync params
class DropboxConfig(BaseModel):
    """Dropbox configuration settings"""
    access_token: Optional[str] = Field(None, description="Dropbox access token")
    oauth: Optional[DropboxOAuthConfig] = Field(None, description="OAuth configuration")
    max_retries: int = Field(3, gt=0, description="Maximum retry attempts for Dropbox operations")
    retry_delay: float = Field(1.0, gt=0, description="Initial retry delay in seconds")
    timeout: int = Field(300, gt=0, description="Request timeout in seconds")
    use_oauth: bool = Field(False, description="Use OAuth instead of direct token")


    @field_validator('access_token')
    @classmethod
    def validate_access_token(cls, v: Optional[str]) -> Optional[str]:
        """Validate Dropbox access token format"""
        if v and not v.startswith(('sl.', 'pk.')):
            logger.warning("Dropbox access token format looks unusual")
        return v

    def get_access_token(self) -> str:
        """Get current access token, refreshing if needed"""
        if self.use_oauth and self.oauth:
            return self._get_oauth_token()
        elif self.access_token:
            return self.access_token
        else:
            raise ValueError("No access token available. Configure either direct token or OAuth.")

    def _get_oauth_token(self) -> str:
        """Get OAuth token, refreshing if necessary"""
        if not self.oauth:
            raise ValueError("OAuth configuration not provided")
        
        try:
            # Load existing token
            with open(self.oauth.token_file, "r") as f:
                token_data = json.load(f)
            
            # Check if token needs refresh (basic check)
            if self._token_needs_refresh(token_data):
                token_data = self._refresh_token(token_data)
            
            return token_data["access_token"]
        
        except FileNotFoundError:
            raise ValueError(f"Token file {self.oauth.token_file} not found. Run OAuth flow first.")
        except Exception as e:
            logger.error(f"Error getting OAuth token: {e}")
            raise

    def _token_needs_refresh(self, token_data: dict) -> bool:
        """Check if token needs refresh"""
        if "issued_at" not in token_data or "expires_in" not in token_data:
            return True
        
        issued_at = token_data["issued_at"]
        expires_in = token_data["expires_in"]
        current_time = datetime.now().timestamp()
        
        # Refresh if token expires within 5 minutes
        return (current_time - issued_at) >= (expires_in - 300)

    def _refresh_token(self, token_data: dict) -> dict:
        """Refresh OAuth token"""
        if not self.oauth:
            raise ValueError("OAuth configuration not available")
        
        refresh_token = token_data.get("refresh_token")
        if not refresh_token:
            raise ValueError("No refresh token available")

        resp = requests.post("https://api.dropbox.com/oauth2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.oauth.client_id,
            "client_secret": self.oauth.client_secret,
        })
        resp.raise_for_status()
        new_data = resp.json()

        # Update token data
        token_data["access_token"] = new_data["access_token"]
        token_data["expires_in"] = new_data.get("expires_in", 14400)
        token_data["issued_at"] = datetime.now().timestamp()

        # Save updated token
        with open(self.oauth.token_file, "w") as f:
            json.dump(token_data, f, indent=4)

        logger.info("Access token refreshed successfully")
        return token_data


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
            # Check for OAuth vs direct token configuration
            use_oauth = os.getenv('DROPBOX_USE_OAUTH', '').lower() == 'true'
            
            if use_oauth:
                # OAuth configuration
                required_oauth_vars = [
                    'DROPBOX_CLIENT_ID',
                    'DROPBOX_CLIENT_SECRET',
                    'DROPBOX_REDIRECT_URI'
                ]
                
                missing_oauth_vars = [var for var in required_oauth_vars if not os.getenv(var)]
                if missing_oauth_vars:
                    raise ValueError(f"Missing OAuth environment variables: {', '.join(missing_oauth_vars)}")
                
                oauth_config = DropboxOAuthConfig(
                    client_id=os.getenv('DROPBOX_CLIENT_ID', ''),
                    client_secret=os.getenv('DROPBOX_CLIENT_SECRET', ''),
                    redirect_uri=os.getenv('DROPBOX_REDIRECT_URI', ''),
                    token_file=os.getenv('DROPBOX_TOKEN_FILE', 'TOKEN.txt')
                )
                
                dropbox_config = DropboxConfig(
                    oauth=oauth_config,
                    use_oauth=True,
                    max_retries=int(os.getenv('DROPBOX_MAX_RETRIES', '3')),
                    retry_delay=float(os.getenv('DROPBOX_RETRY_DELAY', '1.0')),
                    timeout=int(os.getenv('DROPBOX_TIMEOUT', '300'))
                )
            else:
                # Direct token configuration
                if not os.getenv('DROPBOX_ACCESS_TOKEN'):
                    raise ValueError("Missing DROPBOX_ACCESS_TOKEN. Set token directly or use OAuth with DROPBOX_USE_OAUTH=true")
                
                dropbox_config = DropboxConfig(
                    access_token=os.getenv('DROPBOX_ACCESS_TOKEN', ''),
                    use_oauth=False,
                    max_retries=int(os.getenv('DROPBOX_MAX_RETRIES', '3')),
                    retry_delay=float(os.getenv('DROPBOX_RETRY_DELAY', '1.0')),
                    timeout=int(os.getenv('DROPBOX_TIMEOUT', '300'))
                )
        
            # Check for required environment variables
            required_s3_vars = [
                'AWS_ACCESS_KEY_ID',
                'AWS_SECRET_ACCESS_KEY',
                'S3_BUCKET_NAME',
                'AWS_REGION'
            ]

            missing_s3_vars = [var for var in required_s3_vars if not os.getenv(var)]
            if missing_s3_vars:
                raise ValueError(f"Missing required environment variables: {', '.join(missing_s3_vars)}")

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
#choose one of the folloeing dropbox configurations: 

# Option 1: Direct Access Token
DROPBOX_ACCESS_TOKEN=your_dropbox_access_token_here
DROPBOX_USE_OAUTH=false

# Option 2: OAuth Configuration
DROPBOX_USE_OAUTH=true
DROPBOX_CLIENT_ID=your_dropbox_client_id
DROPBOX_CLIENT_SECRET=your_dropbox_client_secret
DROPBOX_REDIRECT_URI=http://localhost:8000/oauth/callback
DROPBOX_TOKEN_FILE=TOKEN.txt

# Common Dropbox settings
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
    if config.dropbox.use_oauth:
        print(f"Dropbox: OAuth mode, client_id={'*' * 10}..., retries={config.dropbox.max_retries}")
    else:
        print(f"Dropbox: token={'*' * 10}..., retries={config.dropbox.max_retries}")
    print(f"S3: bucket={config.s3.bucket_name}, region={config.s3.region}, retries={config.s3.max_retries}")
    print(f"Sync: batch_size={config.sync.batch_size}, log_level={config.sync.log_level}")
    print(f"Temp dir: {config.sync.temp_dir}")
    print(f"Log file: {config.sync.log_file}")