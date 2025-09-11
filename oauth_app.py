import os
import json
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, HTMLResponse
import requests # makes HTTP requests to Dropbox
from datetime import datetime
from dotenv import load_dotenv

load_dotenv() #this loads client ID , secret , redirect URI from env

app = FastAPI(title="Dsync OAuth Server", description="OAuth server for Dropbox integration with dsync")

# Configuration from environment variables
#dropbox app keys
DROPBOX_CLIENT_ID = os.getenv("DROPBOX_CLIENT_ID")
#where dropbox will send users after login
DROPBOX_CLIENT_SECRET = os.getenv("DROPBOX_CLIENT_SECRET")
DROPBOX_REDIRECT_URI = os.getenv("DROPBOX_REDIRECT_URI", "http://localhost:8000/oauth/callback")
TOKEN_FILE = os.getenv("DROPBOX_TOKEN_FILE", "TOKEN.txt")

@app.get("/")
def root():
    """Root endpoint with instructions"""
    html_content = """
    <html>
        <head>
            <title>Dsync OAuth Setup</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                .container { max-width: 600px; }
                .button { background: #0061ff; color: white; padding: 10px 20px; 
                         text-decoration: none; border-radius: 5px; display: inline-block; }
                .status { padding: 10px; margin: 10px 0; border-radius: 5px; }
                .success { background: #d4edda; border: 1px solid #c3e6cb; }
                .warning { background: #fff3cd; border: 1px solid #ffeaa7; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Dsync OAuth Setup</h1>
                <p>This server helps you authenticate with Dropbox for the dsync tool.</p>
                
                <h2>Setup Steps:</h2>
                <ol>
                    <li>Click the button below to start OAuth flow</li>
                    <li>Authorize dsync to access your Dropbox</li>
                    <li>Tokens will be saved automatically</li>
                    <li>Run your dsync tool with OAuth enabled</li>
                </ol>
                
                <a href="/oauth/start" class="button">Start Dropbox Authorization</a>
                
                <h2>Status:</h2>
                <div id="status">
    """
    
    # Check if tokens exist
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            if 'access_token' in token_data:
                html_content += '<div class="status success">✓ Tokens found and ready to use</div>'
            else:
                html_content += '<div class="status warning">⚠ Token file exists but incomplete</div>'
        except:
            html_content += '<div class="status warning">⚠ Token file exists but corrupted</div>'
    else:
        html_content += '<div class="status warning">⚠ No tokens found - authorization needed</div>'
    
    html_content += """
                </div>
                
                <h2>Configuration:</h2>
                <p>Make sure your .env file has:</p>
                <pre>
DROPBOX_USE_OAUTH=true
DROPBOX_CLIENT_ID=your_client_id_here
DROPBOX_CLIENT_SECRET=your_client_secret_here
DROPBOX_REDIRECT_URI=http://localhost:8000/oauth/callback
                </pre>
            </div>
        </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)

#redirects user to dropbox OAuth's page
#requests and authorization code
#requests a refresh token
@app.get("/oauth/start")
def oauth_start():
    """Initiate OAuth authorization flow"""
    if not DROPBOX_CLIENT_ID:
        return {"error": "DROPBOX_CLIENT_ID not configured"}
    
    authorize_url = (
        "https://www.dropbox.com/oauth2/authorize"
        f"?client_id={DROPBOX_CLIENT_ID}"
        f"&redirect_uri={DROPBOX_REDIRECT_URI}"
        "&response_type=code"
        "&token_access_type=offline"  # Request refresh token
    )
    
    return RedirectResponse(authorize_url)

#dropbox redirects after login with the autorization code
#the code is exchanged for access and refresh token
@app.get("/oauth/callback")
def oauth_callback(request: Request, code: str = None):
    """Handle OAuth callback and exchange code for tokens"""
    if not code:
        return {"error": "Authorization code missing"}
    
    if not all([DROPBOX_CLIENT_ID, DROPBOX_CLIENT_SECRET]):
        return {"error": "Dropbox client credentials not configured"}
    
    # Exchange code for tokens
    token_url = "https://api.dropbox.com/oauth2/token"
    data = {
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": DROPBOX_REDIRECT_URI,
        "client_id": DROPBOX_CLIENT_ID,
        "client_secret": DROPBOX_CLIENT_SECRET
    }
    
    try:
        resp = requests.post(token_url, data=data)
        resp.raise_for_status()
        token_data = resp.json()
        
        # Add timestamp for token management
        token_data["issued_at"] = datetime.now().timestamp()
        
        # Save tokens to file
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f, indent=4)
        
        # Success page
        html_content = f"""
        <html>
            <head>
                <title>OAuth Success</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; }}
                    .success {{ background: #d4edda; padding: 20px; border-radius: 5px; }}
                </style>
            </head>
            <body>
                <div class="success">
                    <h2>✓ Authorization Successful!</h2>
                    <p>Tokens have been saved to <code>{TOKEN_FILE}</code></p>
                    <p>You can now run dsync with OAuth enabled.</p>
                    <p><strong>Next steps:</strong></p>
                    <ol>
                        <li>Set <code>DROPBOX_USE_OAUTH=true</code> in your environment</li>
                        <li>Run your dsync tool: <code>python main.py</code></li>
                    </ol>
                </div>
            </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except requests.RequestException as e:
        return {"error": f"Failed to exchange code for tokens: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}

@app.get("/token/status")
def token_status():
    """Check token status"""
    if not os.path.exists(TOKEN_FILE):
        return {"status": "no_tokens", "message": "No token file found"}
    
    try:
        with open(TOKEN_FILE, 'r') as f:
            token_data = json.load(f)
        
        if 'access_token' not in token_data:
            return {"status": "invalid", "message": "Token file incomplete"}
        
        # Check if token might be expired
        issued_at = token_data.get("issued_at", 0)
        expires_in = token_data.get("expires_in", 14400)  # Default 4 hours
        current_time = datetime.now().timestamp()
        
        if (current_time - issued_at) >= expires_in:
            return {
                "status": "expired", 
                "message": "Token may be expired",
                "issued_at": datetime.fromtimestamp(issued_at).isoformat(),
                "expires_in": expires_in
            }
        else:
            return {
                "status": "valid",
                "message": "Token appears to be valid",
                "issued_at": datetime.fromtimestamp(issued_at).isoformat(),
                "expires_in": expires_in
            }
    
    except Exception as e:
        return {"status": "error", "message": f"Error reading token file: {str(e)}"}

#uses refresh token to request a new access token
#updates the TOKEN.txt
@app.post("/token/refresh")
def refresh_token():
    """Manually refresh the access token"""
    try:
        with open(TOKEN_FILE, 'r') as f:
            token_data = json.load(f)
        
        refresh_token = token_data.get("refresh_token")
        if not refresh_token:
            return {"error": "No refresh token available"}
        
        resp = requests.post("https://api.dropbox.com/oauth2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": DROPBOX_CLIENT_ID,
            "client_secret": DROPBOX_CLIENT_SECRET,
        })
        resp.raise_for_status()
        new_data = resp.json()
        
        # Update token data
        token_data["access_token"] = new_data["access_token"]
        token_data["expires_in"] = new_data.get("expires_in", 14400)
        token_data["issued_at"] = datetime.now().timestamp()
        
        # Save updated token
        with open(TOKEN_FILE, "w") as f:
            json.dump(token_data, f, indent=4)
        
        return {
            "message": "Token refreshed successfully",
            "expires_in": token_data["expires_in"],
            "issued_at": datetime.fromtimestamp(token_data["issued_at"]).isoformat()
        }
        
    except Exception as e:
        return {"error": f"Failed to refresh token: {str(e)}"}

if __name__ == "__main__":
    print("Starting OAuth server...")
    print(f"Visit http://localhost:8000 to start OAuth flow")
    print(f"Redirect URI: {DROPBOX_REDIRECT_URI}")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)