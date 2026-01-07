# -*- coding: utf-8 -*-
"""
Remote Access Helper - Expose Dashboard to Internet

This script helps expose your bot dashboard to the internet securely
using Cloudflare Tunnel (cloudflared) or ngrok.

OPTIONS:
1. Cloudflare Tunnel (RECOMMENDED) - Free, secure, custom domain possible
2. ngrok - Quick setup, free tier available

USAGE:
    python tools/remote_access.py cloudflare
    python tools/remote_access.py ngrok
    python tools/remote_access.py install  # Install cloudflared

REQUIREMENTS:
- cloudflared: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/
- ngrok: https://ngrok.com/download

SECURITY NOTE:
The dashboard has emergency stop controls. Ensure you:
1. Use HTTPS (both options provide this)
2. Don't share the URL publicly
3. Consider adding HTTP basic auth if needed
"""
import sys
import os
import subprocess
import shutil

DASHBOARD_PORT = 8080


def check_cloudflared():
    """Check if cloudflared is installed."""
    return shutil.which("cloudflared") is not None


def check_ngrok():
    """Check if ngrok is installed."""
    return shutil.which("ngrok") is not None


def install_cloudflared_windows():
    """Download and setup cloudflared for Windows."""
    print("\n=== Installing cloudflared for Windows ===\n")

    # Download URL for Windows
    url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
    target = os.path.expanduser("~\\cloudflared.exe")

    print(f"Downloading cloudflared to {target}...")

    try:
        import urllib.request
        urllib.request.urlretrieve(url, target)
        print(f"Downloaded to: {target}")
        print("\nTo use, either:")
        print(f"  1. Add {os.path.dirname(target)} to your PATH")
        print(f"  2. Run directly: {target} tunnel --url http://localhost:{DASHBOARD_PORT}")
        print("\nOr use winget: winget install cloudflare.cloudflared")
    except Exception as e:
        print(f"Download failed: {e}")
        print("\nManual install options:")
        print("  1. winget install cloudflare.cloudflared")
        print("  2. Download from: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/")


def run_cloudflare_tunnel():
    """Start Cloudflare quick tunnel (no account needed)."""
    if not check_cloudflared():
        print("cloudflared not found!")
        print("Install with: winget install cloudflare.cloudflared")
        print("Or run: python tools/remote_access.py install")
        return

    print("\n=== Starting Cloudflare Quick Tunnel ===\n")
    print(f"Exposing http://localhost:{DASHBOARD_PORT} to the internet...")
    print("A random *.trycloudflare.com URL will be generated.\n")
    print("SECURITY: This URL is public but random. Don't share it!")
    print("Press Ctrl+C to stop the tunnel.\n")

    try:
        subprocess.run([
            "cloudflared", "tunnel", "--url", f"http://localhost:{DASHBOARD_PORT}"
        ])
    except KeyboardInterrupt:
        print("\nTunnel stopped.")


def run_ngrok_tunnel():
    """Start ngrok tunnel."""
    if not check_ngrok():
        print("ngrok not found!")
        print("Download from: https://ngrok.com/download")
        print("Or install with: winget install ngrok.ngrok")
        return

    print("\n=== Starting ngrok Tunnel ===\n")
    print(f"Exposing http://localhost:{DASHBOARD_PORT} to the internet...")
    print("Press Ctrl+C to stop the tunnel.\n")

    try:
        subprocess.run(["ngrok", "http", str(DASHBOARD_PORT)])
    except KeyboardInterrupt:
        print("\nTunnel stopped.")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nCurrent status:")
        print(f"  cloudflared: {'INSTALLED' if check_cloudflared() else 'NOT FOUND'}")
        print(f"  ngrok: {'INSTALLED' if check_ngrok() else 'NOT FOUND'}")
        return

    cmd = sys.argv[1].lower()

    if cmd == "cloudflare" or cmd == "cf":
        run_cloudflare_tunnel()
    elif cmd == "ngrok":
        run_ngrok_tunnel()
    elif cmd == "install":
        install_cloudflared_windows()
    else:
        print(f"Unknown command: {cmd}")
        print("Use: cloudflare, ngrok, or install")


if __name__ == "__main__":
    main()
