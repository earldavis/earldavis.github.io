# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Personal Jekyll static site (GitHub Pages, `earldavis.github.io`) built on the Gradfolio theme. Content is a blog of data science / data engineering posts, an about page, a resume, and a projects gallery. There is no test suite or linter.

## Commands

```bash
bundle install                 # install Jekyll + plugins from Gemfile
bundle exec jekyll serve       # local dev server at http://localhost:4000 with live rebuild
bundle exec jekyll build       # one-off build into _site/
```

`_site/`, `.jekyll-cache/`, and `.sass-cache/` are build output and gitignored — never edit them.

## Structure

- **Layouts** (`_layouts/`): `compress.html` minifies HTML using the `compress_html` settings in `_config.yml`, but it is currently disabled — the `layout: compress` line is commented out in `default.html` and `clean.html`. `default.html` and `clean.html` are the two full-page shells (nav, favicon, MathJax, social footer, analytics includes); `post.html` wraps `default` and adds Disqus; `about.html` is used by the home page (`index.md`, permalink `/`) with a profile image from `assets/images/`.
- **Pages**: `index.md` (home/bio), `about/index.md`, `resume/index.md` (uses `layout: clean`), `blog/index.html` (paginated post list, 4 per page via `jekyll-paginate`), `archive.html`, and `_pages/` (explicitly `include`d in config: `projects.md` gallery, `404.md`).
- **Posts** (`_posts/`): `YYYY-MM-DD-slug.md` with front matter `layout: post`, `title`, `date`, `categories: [snake_case,]`, `description`. Permalinks are `/blog/:year/:title/`.
- **Projects** (`_projects/`): a Jekyll collection output at `/projects/:name/`; display order is set by the `collections.projects.order` list in `_config.yml`, so new project files must be added there. The current files are still theme placeholders.
- **`_data/`**: not used as Jekyll data (no `site.data` references). It holds drafts and supporting scripts (e.g. a `.md` post draft, `.py` snippets, `resume.json`). Move a draft into `_posts/` to publish it.
- **Styles**: `assets/css/main.scss` imports partials from `assets/css/_sass/` (compressed output).
- **Site settings** (title, description, social handles, plugins) live in `_config.yml`; changes to it require restarting `jekyll serve`.

Markdown is kramdown with `parse_block_html: true` (Markdown inside HTML blocks is rendered) and Rouge syntax highlighting. External links get `target="_blank"` automatically via `jekyll-target-blank`.
