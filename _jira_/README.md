# _jira — Local Task Management

Local replacement for an external issue tracker.
See `.agent/skills/jira_skill.md` for the full workflow and templates.

---

## Quick Reference

```
_jira_/
  epic_{id}_{slug}/
    todo_{tid}_{slug}.md    ← task in progress
    pr_{tid}_{slug}.md      ← task ready for review
    done_{tid}_{slug}.md    ← task accepted (delete after sprint)
    todo_{sid}_{slug}.md    ← story (no pr/done state)
```

IDs are globally unique: `epic_27`, `t11`, `s2`.
Rename the file to change state — do not create a new file.
