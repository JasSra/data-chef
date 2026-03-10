# AI Prompt: Deploying Data Chef Docker Container

Here is a prompt you can feed into an AI assistant (like ChatGPT, GitHub Copilot, or Claude) to generate the infrastructure or docker-compose file required to run your app, connecting it to shared resources like an external Redis cluster.

---

**Copy and Paste the text below to your AI assistant:**

```text
Act as a DevOps Engineer.

I need a deployment configuration (either a `docker run` command, a `docker-compose.yml`, or a Kubernetes `Deployment.yaml`) to run a Next.js standalone application called "data-chef".

Here are the requirements:
1. **Docker Image**: The image is published to our Docker Hub registry at `jassra/datachef:latest`.
2. **Ports**: The container runs on port 3000 natively. Map it to port 80 or 8080 on the host, depending on what's standard for a web facing app.
3. **Shared Resources Context**: This application processes data and uses external services. Critically, we do NOT want this container to spin up its own Redis or DB. It must use our globally shared Redis cluster and API layers.
4. **Environment Variables Needed**:
   - `NODE_ENV=production`
   - `REDIS_HOST=` (needs to point to our shared Redis, e.g. `redis.shared.threadcode.internal`)
   - `REDIS_PORT=6379`
   - `REDIS_PASSWORD=` (Securely injected)
   - Any additional observability or DB connection strings relevant for "Data Chef" tracing, such as App Insights.

Please generate a `docker-compose.yml` that pulls the latest image, sets the restart policy to `always`, and maps these environment variables cleanly. Provide instructions on what commands I need to run to pull and start this stack.
```

