# Kalshi API key setup (read-only, your own key pair)

The recorder needs a Kalshi API key, because Kalshi's live order-book feed (WebSocket) only accepts
authenticated connections, even for public market data. This guide creates a **read-only** key using a
key pair **you generate yourself**, so the private key never leaves your computer.

## How it works

A Kalshi API key has two halves:

| Part | What it is | Secret? | Where it lives |
|---|---|---|---|
| **Key ID** | A UUID Kalshi shows after you create the key, e.g. `b496ed3a-bae2-…` | No. It's an identifier, like a username. | Recorder config / AWS Parameter Store |
| **Private key** | A `.pem` file you generate | **Yes.** Anyone who has it can call the API as you. | Your computer (`~/.kalshi/`), then AWS Parameter Store |

You give Kalshi only the **public** key. The recorder signs each request with the private key, and
Kalshi checks the signature against the public key you uploaded.

**Why generate your own key pair?** Kalshi can also generate the pair for you, but then the private key
downloads through your browser as a `.txt` file and can end up in your Downloads folder, a cloud sync
or a backup. Generating it yourself avoids that. The two methods are otherwise equally secure.

## Prerequisites

- A Kalshi account. You must be 18+ and pass identity verification.
  The docs don't mention a deposit requirement for API keys.
- OpenSSL. It's built into macOS; for Windows see the Windows section below.

## Step 1 — Generate the key pair

Generate it in a folder **outside the repository**, so the private key can never be committed by accident.
The key is RSA 2048-bit in PKCS#8 format, the format Kalshi's documentation uses.

### macOS / Linux (Terminal)

```bash
mkdir -p ~/.kalshi && chmod 700 ~/.kalshi && cd ~/.kalshi

# Private key: keep this secret
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out kalshi_private.pem
chmod 600 kalshi_private.pem

# Public key: this is what you give Kalshi
openssl rsa -in kalshi_private.pem -pubout -out kalshi_public.pem

# Print the public key so you can copy it
cat kalshi_public.pem
```

### Windows

Windows doesn't include OpenSSL. Use **one** of these:

- **Git for Windows** (easiest if Git is installed): it bundles OpenSSL. Use *Git Bash* and run the
  macOS commands above unchanged, or call it from PowerShell as shown below.
- **winget:** `winget install ShiningLight.OpenSSL.Light`, then open a new PowerShell window.

Check which one you have in PowerShell:

```powershell
openssl version
# If it isn't found but Git for Windows is installed, use Git's copy:
Set-Alias openssl "C:\Program Files\Git\usr\bin\openssl.exe"
openssl version
```

Then generate the keys in **PowerShell**:

```powershell
$dir = Join-Path $HOME ".kalshi"          # C:\Users\<you>\.kalshi
New-Item -ItemType Directory -Force -Path $dir | Out-Null
Set-Location $dir

# Private key: keep this secret
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out kalshi_private.pem

# Restrict the private key to your user only (the Windows equivalent of chmod 600)
icacls kalshi_private.pem /inheritance:r /grant:r "$($env:USERNAME):(R,W)"

# Public key: this is what you give Kalshi
openssl rsa -in kalshi_private.pem -pubout -out kalshi_public.pem

# Print the public key, or copy it straight to the clipboard
Get-Content kalshi_public.pem
Get-Content kalshi_public.pem -Raw | Set-Clipboard
```

### Check the files

Both files should begin like this:

- `kalshi_private.pem` → `-----BEGIN PRIVATE KEY-----`
- `kalshi_public.pem` → `-----BEGIN PUBLIC KEY-----`

## Step 2 — Create the API key on Kalshi

1. Log in to kalshi.com and open **Account Settings** (profile icon, top right). The profile page is
   `https://kalshi.com/account/profile`. Find the **API keys** section.
2. Click **Create New API Key** and choose the option to **provide your own public key**.
3. If asked for a key type, choose **RSA**.
4. Paste the **entire** contents of `kalshi_public.pem`, including the
   `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----` lines.
5. Set the permission to **Read only**. The recorder never trades, and a read-only key can't place orders
   or move money even if it leaks.
6. Name it so its owner is obvious, e.g. `<yourname>-api-key`.
7. Create it. The key list now shows it labelled **Read only**, with the **Key ID** (a UUID) underneath.
   Copy the Key ID. If you need it again, it stays visible in the list.

### "Verify your location" notice

The API keys page may show *"API trading defaults to the most restricted access until you verify your
location"*, and may say your current location is prohibited (a VPN can cause this). It restricts
**trading**. This project only reads market data with a read-only key. If market-data requests fail with
an authorization error, check this notice first.

## Step 3 — Store the key for the recorder

**Running locally:** leave the private key at `~/.kalshi/kalshi_private.pem`
(`C:\Users\<you>\.kalshi\kalshi_private.pem` on Windows) and give the recorder the Key ID.
The exact flags or environment variables will be documented in the README when the recorder is built.

**On AWS:** store both values as encrypted SecureString parameters in SSM Parameter Store, which is free.
Run this from the folder containing the key, with AWS CLI credentials for the project account:

```bash
aws ssm put-parameter --name /prediction-market-arb/kalshi/key-id \
  --type SecureString --value "<KEY-ID-UUID>" --overwrite

aws ssm put-parameter --name /prediction-market-arb/kalshi/private-key \
  --type SecureString --value file://kalshi_private.pem --overwrite
```

PowerShell uses the same commands, with a backtick `` ` `` instead of `\` for line continuation.
The recorder on EC2 reads these at startup, and only its IAM role has permission to read them.

## Rotating or revoking a key

1. Create the new key (Steps 1–2) and update the two SSM parameters (Step 3).
2. Restart the recorder service and check that it connects.
3. Delete the old key on Kalshi with the red **✕** next to it in the API keys list.
4. Delete the old `kalshi_private.pem`.

If a private key may have leaked, **delete it on Kalshi first**. That revokes it immediately.

## Do / don't

- **Do** keep a backup of the private key in a password manager if you want one.
- **Don't** commit `.pem` files, paste them into chat, email them, or put them in the repository folder.
- **Don't** share the private key between people. Each person creates their own key from their own account.

## Current key

| Name | Owner | Permission | Created | Notes |
|---|---|---|---|---|
| `dbinoy-api-key` | Binoy | Read only | 2026-11-15 | Temporary. Replace with Vihaan's own key, then delete this one. |

References: [Kalshi API keys documentation](https://docs.kalshi.com/getting_started/api_keys) ·
[Kalshi WebSocket connection](https://docs.kalshi.com/websockets/websocket-connection)
