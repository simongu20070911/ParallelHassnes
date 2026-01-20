# ParallelHassnes Path Migration Audit

- runs_root: `/Users/simongu/ParallelHassnes/runs`
- old_prefix: `/Users/simongu/Desktop/ParallelHassnes`
- new_prefix: `/Users/simongu/ParallelHassnes`
- apply: `True`
- candidates_scanned: `632`
- hits_count: `329`
- skipped_count: `0`
- changed_count: `329`

## Hits By Kind
- `state.json`: `178`
- `current.json`: `131`
- `batch_meta.json`: `8`
- `scoreboard.batch.json`: `5`
- `batch_unreadable.json`: `5`
- `meta.json`: `2`

## Changed By Kind
- `state.json`: `178`
- `current.json`: `131`
- `batch_meta.json`: `8`
- `scoreboard.batch.json`: `5`
- `batch_unreadable.json`: `5`
- `meta.json`: `2`

## Next Steps
- You can remove the Desktop symlink after rewriting if no remaining files reference the old prefix.
- If there are still hits in `state.json`/`meta.json`, that is harmless; operationally the important one is `current.json`.
