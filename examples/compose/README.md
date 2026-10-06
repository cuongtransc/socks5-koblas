# Koblas with Docker Compose

1. Pull or build the image (`mise run build` from the repo root builds `cuongtransc/socks5-koblas:0.2`):

   ```bash
   docker compose pull
   ```

2. Hash a password and create the config:

   ```bash
   docker run --rm cuongtransc/socks5-koblas:0.2 hash "correct-horse-battery-staple"
   cp config.example.toml config.toml
   ```

   `docker run` keeps `config.toml` unmounted until `up`: on macOS (OrbStack), a container
   started after the file was mounted once and then edited can read the old content.

3. Add the hash under `[users]` in `config.toml`:

   ```toml
   [users]
   alice = "$argon2id$v=19$m=19456,t=2,p=1$..."
   ```

4. Start the proxy and test it:

   ```bash
   docker compose up -d
   curl --socks5-hostname alice:correct-horse-battery-staple@127.0.0.1:1080 -I https://example.com
   ```

Koblas reads `config.toml` only at start: run `docker compose restart koblas` after editing it.

| Variable           | Default                          | Purpose                     |
|--------------------|----------------------------------|-----------------------------|
| `KOBLAS_IMAGE`     | `cuongtransc/socks5-koblas:0.2`  | Image to run                |
| `KOBLAS_HOST_PORT` | `1080`                           | Host port mapped to the proxy |

Set them in a `.env` file next to `compose.yaml`. `config.toml` and `.env` are gitignored.
