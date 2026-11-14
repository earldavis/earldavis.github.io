---
layout: post
title: Testing Claude
date: 2026-11-13 19:20:00
categories: [misc,]
description: Testing Claude
---

This post is a small experiment: I'm using Claude Code, Anthropic's command-line coding assistant, to help maintain this site, and I wanted to see how it handles a real (if modest) repository.

## The Setup

This site is a plain Jekyll project hosted on GitHub Pages. There's no build pipeline to speak of, no test suite, and a `_config.yml` that has accumulated a few leftovers from the original theme. In other words, a pretty typical personal site.

## What I Tried

**Documenting the repo.** The first thing I asked for was a `CLAUDE.md` file, which is a short guide that future sessions read to get oriented. It went through the layouts, config, and content folders and wrote up how the pieces fit together. It even caught a few things I'd forgotten, like the fact that HTML compression is turned off because the `layout: compress` line is commented out, and that the projects collection is still full of theme placeholders.

**Previewing the site locally.** Next I asked it to serve the site. A few gems were missing, so it ran `bundle install`, started `jekyll serve`, and then requested the main pages to confirm they actually loaded instead of just assuming the server worked.

**Writing this post.** Finally, I created this file with a placeholder and asked Claude to fill it in. So yes, this post is partly self-referential.

## Takeaways

- It's most useful for the tedious parts: reading through config, remembering commands, and checking that things work.
- It verifies its work by running commands and inspecting the output, which I appreciate.
- It still needs a human in the loop. I review the changes before they get committed, the same way I would with any other contributor.

I'll keep experimenting and may write a follow-up once I've used it on something more substantial, like one of my data engineering pipelines.