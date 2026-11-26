# Live acceptance runbook

This runbook provisions a disposable openGauss endpoint and records the live
evidence that [development-progress.md](development-progress.md) requires. It
has **not been executed in this workspace** (no Docker or WSL is installed
here); the commands follow the official 7.0 container guide and must be
adjusted to the build you actually run. Anything not observed on a real server
stays unchecked in the tracker.

## 1. Prerequisites

- A Linux host, or Docker Desktop using the Linux engine (WSL2). The image is a
  Linux container; Windows containers will not run it.
- Room for the image (about 1.8 GB) and a disposable database.
- Go 1.26 or newer and this checkout.
- `gsql` is not required on the host; the commands below run it in the
  container.

## 2. Start the container

```bash
docker pull opengauss/opengauss-server:latest

# If Docker Hub is unreachable, the official guide offers a mirror:
# docker pull quay.io/opengauss/opengauss-server:latest

docker run --name opengauss-live --privileged=true -d \
  -e GS_PASSWORD='LiveTest#2026' \
  -p 8888:5432 \
  opengauss/opengauss-server:latest
```

`GS_PASSWORD` is required. It must be at least 8 characters and contain upper
case, lower case, a digit and one of `#?!@$%^&*-`. The image creates the `omm`
superuser and trusts local connections; connections from the host must supply
the password, which is the path the driver exercises.

Wait for startup, then confirm the server answers:

```bash
docker logs -f opengauss-live
# The database logs its startup; stop following with Ctrl+C once it is up.

docker exec -u omm opengauss-live bash -lc \
  'gsql -d postgres -p 5432 -c "select version();"'
```

If `gsql` is not on `PATH`, use `/usr/local/opengauss/bin/gsql`. The data
directory is normally `/usr/local/opengauss/data`.

## 3. Create a disposable database and account

```bash
docker exec -u omm opengauss-live bash -lc \
  'gsql -d postgres -p 5432 -c "CREATE DATABASE opengauss_live;"'

docker exec -u omm opengauss-live bash -lc \
  "gsql -d postgres -p 5432 -c \"CREATE USER live_test IDENTIFIED BY 'LiveTest#2026';\""

docker exec -u omm opengauss-live bash -lc \
  'gsql -d postgres -p 5432 -c "GRANT ALL PRIVILEGES ON DATABASE opengauss_live TO live_test;"'
```

Record the authentication the server will request:

```bash
docker exec -u omm opengauss-live bash -lc \
  'gsql -d postgres -p 5432 -c "SHOW password_encryption_type;"'
```

`2` is SHA256 (request 10), `1` is MD5 (request 5) and `3` is SM3 (request 13)
on builds that support it. The first live report should use the default;
record the actual value and the server banner instead of assuming them.

## 4. Run the live suite

PowerShell:

```powershell
$env:OPENGAUSS_DSN = 'host=127.0.0.1 port=8888 user=live_test password=LiveTest#2026 dbname=opengauss_live sslmode=require connect_timeout=10'
go test -run '^TestLiveFirstMilestone$' -v .
```

bash:

```bash
export OPENGAUSS_DSN='host=127.0.0.1 port=8888 user=live_test password=LiveTest#2026 dbname=opengauss_live sslmode=require connect_timeout=10'
go test -run '^TestLiveFirstMilestone$' -v .
```

If the image has TLS disabled, run the first pass with `sslmode=disable` and
record that; the TLS row stays open until a TLS-enabled run exists. Then run
the full suite:

```bash
go test -run '^TestLive' -v .
```

`TestLiveMultiVectorSearch` skips itself when the server lacks the vector type,
and `TestLiveCopyOut` skips when the account cannot create a table. A skip is
not a pass; keep the matching feature row open and record the reason.

## 5. TLS verification

`sslmode=require` proves that TLS was negotiated but does not verify the
server's identity. For `verify-ca` or `verify-full`:

1. Copy the CA that signed the server certificate out of the container, for
   example `docker cp opengauss-live:/usr/local/opengauss/data/cacert.pem
   ./cacert.pem` when that file exists.
2. Use a host name that appears in the certificate's subject alternative
   names.
3. Run with `sslmode=verify-ca sslrootcert=./cacert.pem`, then
   `sslmode=verify-full`.

Keep the mismatched-hostname failure as a separate negative result: a
`verify-full` connection to a name the certificate does not cover must fail,
and a plaintext retry must not happen.

## 6. Record the result

Append one row per server build to the tracker, with enough detail to reproduce
it:

| Field | Example |
| --- | --- |
| Date and commit | 2026-12-01, `309b1c7` |
| Go and target | Go 1.26.0, linux/amd64, CGO disabled |
| Banner | `SELECT version()` verbatim |
| Edition and mode | Lite/full and compatibility mode |
| Authentication | SHA256, MD5, SM3 or GSSAPI |
| TLS | `disable`, `require`, `verify-ca` or `verify-full` |
| Command | `OPENGAUSS_DSN=... go test -run '^TestLive' -v .` with the password redacted |
| Results | Which `TestLive*` passed, skipped or failed |
| Notes | Extensions, missing privileges, certificate caveats |

Mark a tracker box only when its acceptance conditions are actually observed.
An offline suite, a cross-build or a scripted peer does not replace this
evidence.

## 7. Clean up

```bash
docker rm -f opengauss-live
```

## 8. Other environments

- **GSSAPI**: needs a KDC, a ticket and a provider registered with
  `RegisterGSSProvider`; the image alone cannot exercise it. The module ships no
  Kerberos provider.
- **Lite**: needs a Lite server build; request 12 is still unimplemented.
- **HA and `target_session_attrs`**: needs a primary and a standby; see F07.
- **Cross-version**: repeat the run with a 6.0.x image or endpoint and keep the
  two reports separate.

Sources: [openGauss 7.0 container installation](https://docs.opengauss.org/zh/docs/7.0.0/installation_guide/installing_the_container_image.html)
and [7.0.0-RC1 container guide](https://docs.opengauss.org/zh/docs/7.0.0-RC1/docs/InstallationGuide/%E5%AE%B9%E5%99%A8%E9%95%9C%E5%83%8F%E5%AE%89%E8%A3%85.html).
