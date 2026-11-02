---
name: models-and-routing
description: Know which model runs you and how to choose one for a child.
---

Your model is listed in the environment block (`<model>`). A child's model and context come from its role: a `worker` executes in a fork of your context, an `advisor` gives a fresh second opinion on a plan, and a `reviewer`, ideally of a different model family, gives an independent verdict.

The exact tool declarations each model family was trained on are in `.volumes/effective/sys/native/<family>/`.
