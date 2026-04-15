# PushClean — Complete Setup Guide

> ✅ **Brand:** PushClean — consistent across codebase, landing page, and GitHub App.

---

## Table of Contents

1. [Landing Page Status](#1-landing-page-status)
2. [Domain & Hosting](#2-domain--hosting)
3. [Missing Pages](#3-missing-pages)
4. [API Keys Setup](#4-api-keys-setup)
5. [Environment Variables](#5-environment-variables)
6. [GitHub App Setup](#6-github-app-setup)
7. [GitHub Marketplace Submission](#7-github-marketplace-submission)
8. [Cost & Pricing Math](#8-cost--pricing-math)
9. [Launch Checklist](#9-launch-checklist)

---

## 1. Landing Page Status

| Item | Status |
|------|--------|
| Branding (PushClean) | ✅ Done |
| Domain links (pushclean.dev) | ✅ Done |
| Mobile nav fix | ✅ Done |
| JS errors fixed (animateDiff, billing toggle) | ✅ Done |
| Favicon | ❌ Pending |
| OG meta tags (for link previews) | ❌ Pending |
| Email capture backend | ❌ Pending |
| Real install URL on CTA buttons | ❌ Pending (after GitHub App ready) |

### Favicon add karo — `<head>` mein:
```html
<link rel="icon" type="image/png" href="/favicon.png">
```

### OG meta tags add karo — `<head>` mein:
```html
<meta property="og:title" content="PushClean — AI Code Quality, Automated">
<meta property="og:description" content="3 AI models clean your code on every push. Zero setup.">
<meta property="og:image" content="https://pushclean.dev/og-image.png">
<meta property="og:url" content="https://pushclean.dev">
<meta name="twitter:card" content="summary_large_image">
```

### Font fix — index.html mein:
Cabinet Grotesk Google Fonts pe nahi hai — Fontshare pe hai:
```html
<!-- WRONG (current) — Google Fonts pe Cabinet Grotesk nahi hai, 404 aayega -->
<link href="https://fonts.googleapis.com/css2?family=Cabinet+Grotesk...">

<!-- CORRECT — Fontshare CDN use karo -->
<link href="https://api.fontshare.com/v2/css?f[]=cabinet-grotesk@400,500,700,800&display=swap" rel="stylesheet">
```

---

## 2. Domain & Hosting

### Recommended: Vercel (free tier)

```bash
# Install Vercel CLI
npm install -g vercel

# Deploy karo
vercel deploy

# Custom domain attach karo
vercel domains add pushclean.dev
```

DNS settings (apne domain registrar pe):
```
A Record     @    76.76.21.21
CNAME        www  cname.vercel-dns.com
```

---

## 3. Missing Pages

Teen pages banane hain jo GitHub Marketplace require karta hai:

### `/privacy` — Privacy Policy
- Kya data collect karte ho (code, email)
- Kaise store karte ho
- Third party sharing (OpenRouter, GitHub)
- Use karo: **termly.io** ya **privacypolicygenerator.info** — free mein banta hai

### `/terms` — Terms of Service
- Service ki limits
- Refund policy
- Use karo: **getterms.io** — free

### `/docs` — Documentation
Minimum chahiye:
```
- Installation guide
- How it works
- Supported languages
- FAQ
```

---

## 4. API Keys Setup

### OpenRouter (recommended — ek key sab models)

1. `openrouter.ai` pe account banao
2. Billing mein Indian card / UPI add karo
3. API Keys section mein jaake key generate karo
4. Key copy karo — `sk-or-xxxxxxxxxx`

**OpenRouter pe available models:**

| Model | OpenRouter naam | Cost/1K tokens |
|-------|----------------|----------------|
| DeepSeek Chat | `deepseek/deepseek-chat` | ~$0.0001 |
| Claude Sonnet | `anthropic/claude-sonnet-4-6` | ~$0.003 |
| Gemini Flash | `google/gemini-flash-1.5` | ~$0.000075 |

---

## 5. Environment Variables

### Local development — `.env` file banao:

```env
# Core bot (required) — Personal Access Token approach
OPENROUTER_API_KEY=sk-or-xxxxxxxxxxxxxxxxxxxx
GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx
GITHUB_REPO=owner/your-repo
GITHUB_WEBHOOK_SECRET=your_webhook_secret
PUSHCLEAN_DATA_DIR=                        # leave blank for local dev (DBs go in CWD)
PUSHCLEAN_CONFIDENCE_THRESHOLD=0.60

# GitHub App OAuth (future / Marketplace only — NOT needed for the core bot today)
# The bot currently authenticates via GITHUB_TOKEN (PAT) above.
# GITHUB_APP_ID and GITHUB_PRIVATE_KEY are only required once you switch to
# full App-based installation token auth for the Marketplace flow.
# GITHUB_APP_ID=your_app_id
# GITHUB_PRIVATE_KEY=your_private_key
```

### `.gitignore` mein add karo (MOST IMPORTANT):

```gitignore
.env
.env.local
.env.production
__pycache__/
*.pyc
*.db
node_modules/
```

### Code mein use karo:

```python
import os
from dotenv import load_dotenv   # ← FIXED: 'python-dotenv' pip package hai, module 'dotenv' hai

load_dotenv()

api_key = os.getenv("OPENROUTER_API_KEY")
```

**Install karo:**
```bash
pip install python-dotenv   # package naam hai 'python-dotenv'
                             # import naam hai 'dotenv'
```

### Deploy pe (Render):
Render Dashboard → Service → Environment → Add Environment Variable → same keys paste karo

---

## 6. GitHub App Setup

### Step 1 — App banao
`github.com/settings/apps` → New GitHub App

**Fill karo:**
```
App name:        PushClean
Homepage URL:    https://pushclean.dev
Webhook URL:     https://your-backend.onrender.com/webhook
Webhook secret:  (random string generate karo)
```

### Step 2 — Permissions set karo
```
Repository permissions:
  - Contents:       Read & Write
  - Pull requests:  Read & Write
  - Metadata:       Read only (auto-selected)

Subscribe to events:
  - Push
  - Pull request
```

### Step 3 — Private key generate karo
App settings → "Generate a private key" → `.pem` file download hogi
Ye `.env` mein daalo as `GITHUB_PRIVATE_KEY`

### Step 4 — Test karo
Apne ek test repo pe app install karo → code push karo → webhook fire hona chahiye

---

## 7. GitHub Marketplace Submission

### Pre-submission checklist:
- [ ] App actually installs on a test repo
- [ ] Webhook receives push events correctly  
- [ ] Bot comments on PR successfully
- [ ] `pushclean.dev/privacy` live hai
- [ ] `pushclean.dev/terms` live hai
- [ ] Logo ready hai (400×400px PNG)
- [ ] Pricing plans match landing page exactly

### Submission:
`github.com/settings/apps/YOUR_APP` → Marketplace → "List this app"

**Listing mein likhna:**

```
Tagline:
AI-powered code quality on every GitHub push

Description:
PushClean uses 3 AI models (DeepSeek + Claude + Gemini) 
to automatically clean your code on every push. 
Zero config. Real fixes. You review, you merge.

How it works:
1. Install PushClean on your repo
2. Push code to any branch
3. PushClean opens a PR with improvements
4. Review the diff, merge if happy
```

### Approval timeline:
- First review: **3–7 business days**
- Rejection = queue mein wapas jaana
- **First try mein pass karna important hai**

---

## 8. Cost & Pricing Math

### API cost per file (approximate):
```
DeepSeek:  $0.0001
Claude:    $0.003
Gemini:    $0.000075
Total:     ~$0.003 per file (all 3 models)
```

### Free plan cost (20 files/month):
```
20 files × $0.003 = $0.06 per free user/month
100 free users    = $6/month API cost
```

### Break-even calculation:
```
Pro plan:    $12/month revenue
API cost:    ~$0.60/month per Pro user (200 files)
Profit:      ~$11.40 per Pro user/month
```

### When to upgrade from free tier:
```
0–50 users:    Free tiers easily cover it
50–200 users:  ~$10–15/month API costs
200+ users:    Pro subscriptions cover everything
```

---

## 9. Launch Checklist

### Before submitting to Marketplace:
- [ ] Landing page live on `pushclean.dev`
- [ ] Favicon added
- [ ] Cabinet Grotesk font fixed (Fontshare CDN)
- [ ] OG image added
- [ ] `/privacy` page live
- [ ] `/terms` page live
- [ ] `/docs` page live
- [ ] `.env` setup locally with `python-dotenv`
- [ ] `.gitignore` has `.env` and `*.db`
- [ ] OpenRouter key working (or direct API keys)
- [ ] GitHub App created and tested
- [ ] Full flow tested: push → webhook → PR comment
- [ ] Pricing plans match Marketplace listing
- [ ] requirements.txt present and tested on fresh machine
- [ ] Brand name: PushClean ✅ (consistent across all files)

### After Marketplace approval:
- [ ] Update all CTA button links on landing page to real install URL
- [ ] Email capture connected to backend (Mailchimp/ConvertKit)
- [ ] Ticker bar connected to real user count
- [ ] Post on Twitter/X, IndieHackers, ProductHunt

---

## Quick Reference

| What | Where |
|------|-------|
| OpenRouter dashboard | openrouter.ai/dashboard |
| GitHub App settings | github.com/settings/apps |
| GitHub Marketplace | github.com/marketplace |
| Render deploy | render.com/dashboard |
| Privacy policy generator | termly.io |
| Terms generator | getterms.io |
| OG image maker | og-image.vercel.app |
| Cabinet Grotesk font | fontshare.com/fonts/cabinet-grotesk |

---

*Last updated: April 2026 — PushClean v0.1*
