# FlareSolverr Setup Guide for Riyasewana Scraper

## What is FlareSolverr?
FlareSolverr is a proxy server to bypass Cloudflare and other anti-bot protections. It uses headless Chrome/Firefox browsers with additional stealth plugins.

## Prerequisites
- Docker Desktop installed (for Windows/Mac) or Docker Engine (for Linux)
- Python 3.7+ with pip

## Step 1: Install and Run FlareSolverr

### Option A: Using Docker (Recommended)
```bash
# Pull and run FlareSolverr container
docker run -d \
  --name flaresolverr \
  -p 8191:8191 \
  -e LOG_LEVEL=info \
  --restart unless-stopped \
  ghcr.io/flaresolverr/flaresolverr:latest
```

For Windows PowerShell:
```powershell
docker run -d `
  --name flaresolverr `
  -p 8191:8191 `
  -e LOG_LEVEL=info `
  --restart unless-stopped `
  ghcr.io/flaresolverr/flaresolverr:latest
```

### Option B: Using Docker Compose
Create a `docker-compose.yml` file:
```yaml
version: '3.8'
services:
  flaresolverr:
    image: ghcr.io/flaresolverr/flaresolverr:latest
    container_name: flaresolverr
    environment:
      - LOG_LEVEL=info
      - LOG_HTML=false
      - CAPTCHA_SOLVER=none
      - TZ=Asia/Colombo
    ports:
      - "8191:8191"
    restart: unless-stopped
```

Then run:
```bash
docker-compose up -d
```

## Step 2: Verify FlareSolverr is Running

Check if FlareSolverr is healthy:
```bash
curl http://localhost:8191/health
```

Or open in browser: http://localhost:8191

You should see a response like:
```json
{"status": "ok", "message": "FlareSolverr is ready!", "version": "3.3.13"}
```

## Step 3: Install Python Requirements

```bash
pip install beautifulsoup4 pandas tqdm requests lxml
```

## Step 4: Run the Scraper

```bash
python scrape_flaresolverr.py
```

## Docker Commands Reference

### Check if FlareSolverr is running:
```bash
docker ps | grep flaresolverr
```

### View FlareSolverr logs:
```bash
docker logs flaresolverr
```

### Stop FlareSolverr:
```bash
docker stop flaresolverr
```

### Start FlareSolverr:
```bash
docker start flaresolverr
```

### Remove FlareSolverr container:
```bash
docker stop flaresolverr
docker rm flaresolverr
```

## Troubleshooting

### Error: "Could not connect to FlareSolverr"
1. Make sure Docker is running
2. Check if container is running: `docker ps`
3. Check logs: `docker logs flaresolverr`
4. Ensure port 8191 is not used by another application

### Error: "Timeout" or slow responses
1. Increase timeout in the script (maxTimeout parameter)
2. Check Docker resources (CPU/Memory)
3. Restart FlareSolverr: `docker restart flaresolverr`

### Error: "Challenge not solved"
1. Some Cloudflare challenges might be too complex
2. Try updating FlareSolverr to latest version:
   ```bash
   docker pull ghcr.io/flaresolverr/flaresolverr:latest
   docker stop flaresolverr
   docker rm flaresolverr
   # Then run the docker run command again
   ```

## Advanced Configuration

### Using Proxy with FlareSolverr
Add proxy environment variables when running Docker:
```bash
docker run -d \
  --name flaresolverr \
  -p 8191:8191 \
  -e LOG_LEVEL=info \
  -e PROXY_URL=http://proxy-server:port \
  -e PROXY_USERNAME=username \
  -e PROXY_PASSWORD=password \
  --restart unless-stopped \
  ghcr.io/flaresolverr/flaresolverr:latest
```

### Increase Browser Timeout
For slow connections, increase timeout:
```bash
docker run -d \
  --name flaresolverr \
  -p 8191:8191 \
  -e LOG_LEVEL=info \
  -e BROWSER_TIMEOUT=90000 \
  --restart unless-stopped \
  ghcr.io/flaresolverr/flaresolverr:latest
```

## Notes for Production Use

1. **Rate Limiting**: Add appropriate delays between requests
2. **Session Management**: Use sessions for better cookie handling
3. **Error Handling**: Implement proper retry logic
4. **Logging**: Enable detailed logging for debugging
5. **Resources**: Monitor Docker container resources

## Legal and Ethical Considerations

- Always respect website's robots.txt
- Don't overload servers with too many requests
- Check website's Terms of Service
- Use scraped data responsibly
- Consider reaching out to website owners for API access
