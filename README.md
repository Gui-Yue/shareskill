# Skill Search Frontend

A simple Next.js frontend for browsing and searching skills backed by a
SQLite database loaded in the browser via sql.js.

## Features
- List/search skills with tags, categories, and metadata
- Detail page with description, usage, and download links
- Works with external `skill.db` hosting

## Quick Start
1) Install dependencies
```bash
pnpm install
```

2) Configure the database URL (recommended)
Create `.env.local`:
```bash
NEXT_PUBLIC_SKILL_DB_URL=https://example.com/skill.db
```

If not set, the app falls back to `/skill.db` in `public/`.

3) Run dev server
```bash
pnpm dev
```

## Build
```bash
pnpm build
pnpm start
```

## Notes
- For local testing, you can set `NEXT_PUBLIC_SKILL_DB_URL=/skill.db` and
  place the file at `public/skill.db`.
- The GitHub icon in the header links to the project repo.
